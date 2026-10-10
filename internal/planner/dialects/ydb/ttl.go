package ydb

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbttl"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's TTL.
//
// The TTL is the YDB owner's facet of the table, and the owner plans its
// change: `ALTER TABLE t SET (TTL = ...)` or `ALTER TABLE t RESET (TTL)`, one
// statement either way. Where that statement goes is this planner's: after the
// table's added columns, because a TTL may read a column the plan adds, and
// before its dropped ones, because YDB refuses to drop the column a TTL reads
// (`Can't drop TTL column: 'ts', disable TTL first`, measured on 25.1.4.7 and
// 26.2.1.14) and drops it once the TTL reads another column or none. The
// shared feature graph places a table's owned operations after every common
// step, column drops included, so the owner's statement is planned here, per
// table, and written at its place in [Planner.changeTable].

// planTableFacets plans the facet changes of every table the plan changes in
// place, through the owners the runtime selects, and returns each table's
// statements keyed by its identity. A rebuilt table writes its declared TTL
// into its new CREATE TABLE, so its facet changes are not planned here.
func (p *Planner) planTableFacets(
	ctx context.Context,
	runtime featureplan.Runtime,
	diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) (map[string][]ast.Node, error) {
	nodes := make(map[string][]ast.Node)
	builder := objectidentity.NewBuilder(semantics)
	for _, table := range diff.TablesModified {
		key := semantics.TableIdentityKey(table.TableName)
		changes, _ := splitFacetChanges(table.FeatureChanges)
		if _, rebuilt := rebuilds[key]; rebuilt || len(changes) == 0 {
			continue
		}
		subject := builder.Table(table.TableName)
		request := featureplan.Request{
			Target: platform.YDB, Identifiers: semantics, Capabilities: p.caps, Changes: changes,
			Tables:       []featureplan.Table{{Subject: subject, Desired: table.Desired, Current: table.Current}},
			DatabasePath: diff.CurrentDatabasePath,
		}
		features, err := featurehost.Plan(ctx, runtime, request, map[objectidentity.Key]string{subject.Key(): table.TableName})
		if err != nil {
			return nil, err
		}
		if len(features.Rewrites) != 0 {
			return nil, fmt.Errorf("%w: a table facet has no common step to rewrite", schemaext.ErrInvalidValue)
		}
		plan, err := plangraph.Schedule(ctx, features.Contributions...)
		if err != nil {
			return nil, err
		}
		for _, step := range plan.Steps {
			nodes[key] = append(nodes[key], step.Payload...)
		}
	}
	return nodes, nil
}

// splitFacetChanges separates the changes of a table's own facets, whose
// subject is the table, from those of its named children, such as a
// changefeed.
func splitFacetChanges(changes []schemaext.ChangeRecord) (facets, children []schemaext.ChangeRecord) {
	for _, change := range changes {
		if change.Subject.Kind == objectidentity.KindTable {
			facets = append(facets, change)
			continue
		}
		children = append(children, change)
	}
	return facets, children
}

// declaredTTL is the TTL a table's facets declare, or nil for none. A column
// table's tiered TTL is its own declaration and is not this value.
func declaredTTL(facets schemaext.Facets) (*ydbschema.DesiredTTL, error) {
	value, _, err := schemaext.FacetAs[*ydbschema.DesiredTTL](facets, ydbschema.TTLKind)
	return value, err
}

// refuseDroppingTheTTLColumn refuses dropping the column a table's TTL reads
// while the TTL stays: YDB refuses it (`Can't drop TTL column`). A declaration
// that names such a column is refused by the TTL's owner, so this is reached
// by a TTL the declaration does not describe and keeps from the database, as an
// HCL document does.
func refuseDroppingTheTTLColumn(tableDiff difftypes.TableDiff) error {
	policy, err := declaredTTL(tableDiff.Desired.Table.Facets)
	if err != nil || policy == nil {
		return err
	}
	column := policy.Policy.Column
	if !slices.ContainsFunc(tableDiff.ColumnsRemoved, func(field schemamodel.Field) bool { return field.Name == column }) {
		return nil
	}
	return refuseFact(fmt.Sprintf("dropping column %q of table %q", column, tableDiff.TableName),
		"the table's TTL reads it, and YDB refuses to drop the column a TTL reads (`Can't drop TTL column`); "+
			"remove the TTL, or move it to another column, first")
}

// refuseTTLColumn holds the column a column table's tiered TTL reads to the
// types YDB reads a TTL from, through the type map the renderer writes the
// column with. A modification that carries no declaration of the table is
// left to the server, which refuses the statement by itself.
func (p *Planner) refuseTTLColumn(subject string, declaration schemacapture.TableDeclaration, column, unit string) error {
	if !declaration.HasTable() {
		return nil
	}
	index := slices.IndexFunc(declaration.Fields, func(field schemamodel.Field) bool { return field.Name == column })
	if index < 0 {
		return refuseFact(subject, fmt.Sprintf("it reads column %q, which the table does not declare "+
			"(`Cannot enable TTL on unknown column`)", column))
	}
	canonical, err := ydbttl.Unit(unit)
	if err != nil {
		return refuseFact(subject, err.Error())
	}
	// A type the map refuses is the column's refusal, which is reported where
	// the column is written.
	if mapping, mapErr := ydbtype.Map(declaration.Fields[index].Type, p.caps); mapErr == nil {
		if reason := ydbttl.ColumnRefusal(column, mapping.Type, canonical); reason != "" {
			return refuseFact(subject, reason)
		}
	}
	return nil
}

// recordsSetting reports whether the read of the database recorded a setting
// of kind on table as not described. A record naming the whole kind counts: a
// read that did not look at any table's setting cannot say this table has
// none.
//
// A format's limit does not count. It is what a document's loader records for
// a family the format has no spelling for -- an HCL document standing for the
// current state cannot say whether a table has a TTL -- and it says nothing
// about a table carrying one, where these records are read as exactly that.
func recordsSetting(set coverage.Set, kind coverage.Kind, table schemamodel.Table) bool {
	canonical := tableref.Canonical(table.Schema, table.Name)
	return slices.ContainsFunc(set.Objects, func(object coverage.Object) bool {
		formatLimit := object.Reason == coverage.Unsupported && object.Provenance == coverage.DerivedFromFact
		return object.Kind == kind && !formatLimit &&
			(object.WholeKind() || object.Name == canonical || strings.HasPrefix(object.Name, canonical+"/"))
	})
}
