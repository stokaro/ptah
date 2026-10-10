package ydb

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbindex"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) refuseColumnTableChange(table difftypes.TableDiff) error {
	change := table.YDBColumnTableChange
	if change == nil {
		return nil
	}
	subject := fmt.Sprintf("table %q", table.TableName)
	if !p.caps.Has(capability.ColumnStoreTables) {
		return refuseKey(capability.ColumnStoreTables, subject)
	}
	if change.Desired == nil || change.Current == nil {
		return refuseFact(subject, "changing between row and column storage requires an explicit data migration")
	}
	if err := ydbcolumn.Validate(change.Desired); err != nil {
		return refuseFact(subject, err.Error())
	}
	if (change.Desired.Partitions != 0 && change.Desired.Partitions != change.Current.Partitions) ||
		(len(change.Desired.HashColumns) > 0 && !slices.Equal(change.Desired.HashColumns, change.Current.HashColumns)) {
		return refuseFact(subject, "changing a column table's hash key or shard count requires an explicit data migration")
	}
	if change.Desired.TTL != nil && !p.caps.Has(capability.TieredTTL) {
		return refuseKey(capability.TieredTTL, subject)
	}
	if change.Desired.TTL != nil {
		ttl := change.Desired.TTL
		return p.refuseTTLColumn(subject, table.Desired, ttl.Column, ttl.Unit)
	}
	return nil
}

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
	resets := make(map[string]bool)
	policies := make(map[string]*ast.YDBTieredTTLSpec)
	previous := make(map[string]*ast.YDBTieredTTLSpec)
	for _, table := range diff.DeclaredTables {
		if table.YDBColumnTable == nil || table.YDBColumnTable.TTL == nil {
			continue
		}
		for _, tier := range table.YDBColumnTable.TTL.Tiers {
			source, found := ydbexternal.ResolveSource(diff.CurrentDatabasePath, tier.ExternalSource)
			if found && sources.removed[source.Key()] {
				return columnTTLPlan{}, refuseFact("table "+table.QualifiedName(), "its TTL reads external data source "+tier.ExternalSource+", which the plan drops")
			}
			if found && sources.changed[source.Key()] && !added[table.QualifiedName()] {
				resets[table.QualifiedName()] = true
				policies[table.QualifiedName()] = table.YDBColumnTable.TTL
				previous[table.QualifiedName()] = table.YDBColumnTable.TTL
			}
		}
	}
	changedColumnTTLPolicies(diff.TablesModified, resets, policies, previous)
	if len(policies) > 0 && !p.caps.Has(capability.TieredTTL) {
		return columnTTLPlan{}, refuseKey(capability.TieredTTL, "restoring column-table TTL after source replacement")
	}
	plan := columnTTLPlan{reads: commonReads{}}
	for _, name := range slices.Sorted(maps.Keys(resets)) {
		node := columnTTLStatement(name, nil)
		plan.before = append(plan.before, node)
		plan.reads.add(node, ydbscheme.TieredTTLReads(diff.CurrentDatabasePath, previous[name]), true)
	}
	for _, name := range slices.Sorted(maps.Keys(policies)) {
		node := columnTTLStatement(name, policies[name])
		plan.after = append(plan.after, node)
		plan.reads.add(node, ydbscheme.TieredTTLReads(diff.CurrentDatabasePath, policies[name]), false)
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

func columnTTLStatement(name string, policy *ast.YDBTieredTTLSpec) ast.Node {
	statement := "ALTER TABLE " + ydbexternal.Path(name)
	if policy == nil {
		return ast.NewRawSQL(statement + " RESET (TTL)")
	}
	return ast.NewRawSQL(statement + " SET (TTL = " + ydbcolumn.TTLClause(policy, func(name string) string { return sqlident.Quote("ydb", name) }) + ")")
}

// refuseColumnTTLShape checks the complete desired table, including changes
// that remove a TTL column or its min-max index without changing the policy.
func refuseColumnTTLShape(table schemacapture.TableDeclaration) error {
	spec := table.Table.YDBColumnTable
	if spec == nil {
		return nil
	}
	column := ""
	rowTTL, err := declaredTTL(table.Table.Facets)
	if err != nil {
		return err
	}
	if spec.TTL != nil {
		column = spec.TTL.Column
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

func splitColumnTTLCreations(nodes []ast.Node) (early, late []ast.Node) {
	for _, node := range nodes {
		table, ok := node.(*ast.CreateTableNode)
		if ok && table.YDBColumnTable != nil && table.YDBColumnTable.TTL != nil {
			late = append(late, node)
		} else {
			early = append(early, node)
		}
	}
	return early, late
}

// dropRemovedTables drops every table the plan removes, and records the
// external data sources each one's tiered TTL reads, as an early reader: the
// table uses the source as it was, so the source's owner drops, recreates or
// replaces it only after the table is gone, which YDB requires of a drop. The
// engine refuses a removal captured with no table, so each one names its
// policy.
func dropRemovedTables(diff *difftypes.SchemaDiff, reads commonReads) []ast.Node {
	var nodes []ast.Node
	for _, removal := range diff.TablesRemoved {
		node := ast.NewDropTable(removal.Name)
		nodes = append(nodes, node)
		if column := removal.Current.Table.YDBColumnTable; column != nil {
			reads.add(node, ydbscheme.TieredTTLReads(diff.CurrentDatabasePath, column.TTL), true)
		}
	}
	return nodes
}

// changedColumnTTLPolicies adds the tables whose policy the diff changes: the
// RESET of the policy each holds, recorded in previous, and the SET of the one
// it declares.
func changedColumnTTLPolicies(changes []difftypes.TableDiff, resets map[string]bool, policies, previous map[string]*ast.YDBTieredTTLSpec) {
	for _, change := range changes {
		column := change.YDBColumnTableChange
		if column == nil || column.Current == nil || column.Desired == nil || ydbcolumn.TTLEqual(column.Current.TTL, column.Desired.TTL) {
			continue
		}
		if column.Current.TTL != nil {
			resets[change.TableName] = true
			previous[change.TableName] = column.Current.TTL
		}
		if column.Desired.TTL != nil {
			policies[change.TableName] = column.Desired.TTL
		}
	}
}
