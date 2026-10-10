package ydb

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbindex"
	"ptah.run/migration/schemadiff/difftypes"
)

// columnTTLPlan detaches existing policies before source replacement or column
// removal, then installs desired policies after indexes and sources exist.
//
// Its statements are raw SQL, so the plan records what each one reads in
// reads: a policy reads the external data sources its tiers move rows to.
// The data sources belong to their owner, and these reads order it: a
// replacement or a drop of a source follows the RESET of every policy that
// reads it, and its creation or replacement precedes the SET. A RESET is an
// early reader, which uses the source as it was and runs before its
// replacement (see [plangraph.LifecycleDependencies]).
type columnTTLPlan struct {
	before, after []ast.Node
	reads         commonReads
}

func (p *Planner) planColumnTTL(diff *difftypes.SchemaDiff) (columnTTLPlan, error) {
	sources := externalSourceChanges(diff)
	added := make(map[string]bool)
	for _, table := range diff.TablesAdded {
		added[table.Name] = true
	}
	// The owner of a table's column storage plans the RESET and the SET of a
	// policy the diff changes; see [ptah.run/dialect/ydb/ydbplan.ColumnStoreService].
	owned := make(map[string]bool)
	for _, table := range diff.TablesModified {
		if slices.ContainsFunc(table.FeatureChanges, func(record schemaext.ChangeRecord) bool {
			_, ok := record.Value.(*ydbdiff.ColumnStore)
			return ok
		}) {
			owned[table.TableName] = true
		}
	}
	policies := make(map[string]*ydbschema.TieredTTL)
	for _, table := range diff.DeclaredTables {
		store, err := ydbschema.DeclaredColumnStore(table.Facets)
		if err != nil {
			return columnTTLPlan{}, err
		}
		if store == nil || store.TTL == nil {
			continue
		}
		for _, tier := range store.TTL.Tiers {
			source, found := ydbexternal.ResolveSource(diff.CurrentDatabasePath, tier.ExternalSource)
			if found && sources.removed[source.Key()] {
				return columnTTLPlan{}, refuseFact("table "+table.QualifiedName(), "its TTL reads external data source "+tier.ExternalSource+", which the plan drops")
			}
			if found && sources.changed[source.Key()] && !added[table.QualifiedName()] && !owned[table.QualifiedName()] {
				policies[table.QualifiedName()] = store.TTL
			}
		}
	}
	if len(policies) > 0 && !p.caps.Has(capability.TieredTTL) {
		return columnTTLPlan{}, refuseKey(capability.TieredTTL, "restoring column-table TTL after source replacement")
	}
	plan := columnTTLPlan{reads: commonReads{}}
	for _, name := range slices.Sorted(maps.Keys(policies)) {
		reads := ydbscheme.TieredTTLReads(diff.CurrentDatabasePath, policies[name])
		reset := columnTTLStatement(name, nil)
		plan.before = append(plan.before, reset)
		plan.reads.add(reset, reads, true)
		set := columnTTLStatement(name, policies[name])
		plan.after = append(plan.after, set)
		plan.reads.add(set, reads, false)
	}
	return plan, nil
}

// sourceChanges are the external data sources the diff's feature changes
// drop for good and change, by identity.
type sourceChanges struct {
	removed, changed map[objectidentity.Key]bool
}

// externalSourceChanges reads the data source changes the diff hands their
// owner, which the column-table policies are ordered against.
func externalSourceChanges(diff *difftypes.SchemaDiff) sourceChanges {
	sources := sourceChanges{removed: make(map[objectidentity.Key]bool), changed: make(map[objectidentity.Key]bool)}
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*ydbdiff.ExternalDataSource)
		if !ok {
			continue
		}
		key := record.Subject.Key()
		sources.removed[key] = change.Before != nil && change.After == nil
		sources.changed[key] = change.Before != nil && change.After != nil
	}
	return sources
}

func columnTTLStatement(name string, policy *ydbschema.TieredTTL) ast.Node {
	statement := "ALTER TABLE " + ydbexternal.Path(name)
	if policy == nil {
		return ast.NewRawSQL(statement + " RESET (TTL)")
	}
	return ast.NewRawSQL(statement + " SET (TTL = " + ydbcolumn.TTLClause(policy, func(name string) string { return sqlident.Quote("ydb", name) }) + ")")
}

// refuseColumnTTLShape checks the complete desired table, including changes
// that remove a TTL column or its min-max index without changing the policy.
func refuseColumnTTLShape(table schemacapture.TableDeclaration) error {
	store, err := ydbschema.DeclaredColumnStore(table.Table.Facets)
	if err != nil || store == nil {
		return err
	}
	column := ""
	rowTTL, err := declaredTTL(table.Table.Facets)
	if err != nil {
		return err
	}
	if store.TTL != nil {
		column = store.TTL.Column
	} else if rowTTL != nil {
		column = rowTTL.Policy.Column
	}
	key := table.Table.PrimaryKey
	if len(key) == 0 {
		for _, field := range table.Fields {
			if field.Primary {
				key = append(key, field.Name)
			}
		}
	}
	var minMax []string
	for _, index := range table.Indexes {
		if kind, _ := ydbindex.KindOf(index.Type); kind == ydbindex.LocalMinMax {
			minMax = append(minMax, index.Fields...)
		}
	}
	if reason := ydbcolumn.TTLColumnRefusal(column, key, minMax); reason != "" {
		return refuseFact("table "+table.Table.QualifiedName(), reason)
	}
	return nil
}

func splitColumnTTLCreations(nodes []ast.Node) (early, late []ast.Node, err error) {
	for _, node := range nodes {
		var store *ydbschema.DesiredColumnStore
		if table, ok := node.(*ast.CreateTableNode); ok {
			if store, err = ydbschema.DeclaredColumnStore(table.Facets); err != nil {
				return nil, nil, err
			}
		}
		if store != nil && store.TTL != nil {
			late = append(late, node)
		} else {
			early = append(early, node)
		}
	}
	return early, late, nil
}

// dropRemovedTables drops every table the plan removes, and records the
// external data sources each one's tiered TTL reads, as an early reader: the
// table uses the source as it was, so the source's owner drops, recreates or
// replaces it only after the table is gone, which YDB requires of a drop. The
// engine refuses a removal captured with no table, so each one names its
// policy.
func dropRemovedTables(diff *difftypes.SchemaDiff, reads commonReads) ([]ast.Node, error) {
	var nodes []ast.Node
	for _, removal := range diff.TablesRemoved {
		node := ast.NewDropTable(removal.Name)
		nodes = append(nodes, node)
		store, held, err := schemaext.FacetAs[*ydbschema.ObservedColumnStore](removal.Current.Table.Facets, ydbschema.ColumnStoreKind)
		if err != nil {
			return nil, err
		}
		if held {
			reads.add(node, ydbscheme.TieredTTLReads(diff.CurrentDatabasePath, store.TTL), true)
		}
	}
	return nodes, nil
}

// tableStatements are the plan's DROP TABLE statements, and the CREATE TABLE
// statements it writes early and late: a column table whose tiered TTL reads
// a data source is created after the common statements.
type tableStatements struct {
	removed, early, late []ast.Node
}

// tableLifecycle returns the plan's table drops and creations, recording what
// each statement reads in reads.
func (p *Planner) tableLifecycle(
	diff *difftypes.SchemaDiff,
	inlineIndexes map[string]bool,
	sequences map[string][]*ast.AlterSerialSequenceNode,
	semantics identifier.Semantics,
	reads commonReads,
) (tableStatements, error) {
	removed, err := dropRemovedTables(diff, reads)
	if err != nil {
		return tableStatements{}, err
	}
	early, late, err := splitColumnTTLCreations(p.createTables(diff, inlineIndexes, sequences, semantics))
	return tableStatements{removed: removed, early: early, late: late}, err
}
