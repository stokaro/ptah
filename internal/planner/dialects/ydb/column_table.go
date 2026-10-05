package ydb

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbexternal"
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
		return p.refuseTTLColumn(subject, table.Desired, &ast.RowDeletionPolicySpec{Column: ttl.Column, Unit: ttl.Unit, Interval: ttl.Tiers[len(ttl.Tiers)-1].Interval})
	}
	return nil
}

// columnTTLPlan detaches existing policies before source replacement or column
// removal, then installs desired policies after indexes and sources exist.
type columnTTLPlan struct{ before, after []ast.Node }

func (p *Planner) planColumnTTL(diff *difftypes.SchemaDiff) (columnTTLPlan, error) {
	changedSources := make(map[string]bool)
	removedSources := make(map[string]bool)
	for _, source := range diff.ExternalDataSourcesRemoved {
		removedSources[sourcePath(source)] = true
	}
	for _, change := range diff.ExternalDataSourcesChanged {
		changedSources[sourcePath(change.Current)] = true
	}
	added := make(map[string]bool)
	for _, table := range diff.TablesAdded {
		added[table.Name] = true
	}
	resets := make(map[string]bool)
	policies := make(map[string]*ast.YDBTieredTTLSpec)
	for _, table := range diff.DeclaredTables {
		if table.YDBColumnTable == nil || table.YDBColumnTable.TTL == nil {
			continue
		}
		for _, tier := range table.YDBColumnTable.TTL.Tiers {
			source := ydbexternal.RelativePath(tier.ExternalSource, diff.CurrentDatabasePath)
			if removedSources[source] {
				return columnTTLPlan{}, refuseFact("table "+table.QualifiedName(), "its TTL reads external data source "+tier.ExternalSource+", which the plan drops")
			}
			if changedSources[source] && !added[table.QualifiedName()] {
				resets[table.QualifiedName()] = true
				policies[table.QualifiedName()] = table.YDBColumnTable.TTL
			}
		}
	}
	changedColumnTTLPolicies(diff.TablesModified, resets, policies)
	if len(policies) > 0 && !p.caps.Has(capability.TieredTTL) {
		return columnTTLPlan{}, refuseKey(capability.TieredTTL, "restoring column-table TTL after source replacement")
	}
	var plan columnTTLPlan
	for _, name := range slices.Sorted(maps.Keys(resets)) {
		plan.before = append(plan.before, columnTTLStatement(name, nil))
	}
	for _, name := range slices.Sorted(maps.Keys(policies)) {
		plan.after = append(plan.after, columnTTLStatement(name, policies[name]))
	}
	return plan, nil
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
func refuseColumnTTLShape(table difftypes.TableDeclaration) error {
	spec := table.Table.YDBColumnTable
	if spec == nil {
		return nil
	}
	column := ""
	if spec.TTL != nil {
		column = spec.TTL.Column
	} else if !table.Table.RowDeletionPolicy.IsZero() {
		column = table.Table.RowDeletionPolicy.Column
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

// A removed column table may reference a removed source. The diff carries no
// declaration for removed tables, so all table drops precede source drops.
func removedTablesBeforeSources(diff *difftypes.SchemaDiff, external externalPlan) []ast.Node {
	if len(external.drops) == 0 {
		return nil
	}
	return removedTableNodes(diff.TablesRemoved)
}
func removedTablesAfterSources(diff *difftypes.SchemaDiff, external externalPlan) []ast.Node {
	if len(external.drops) > 0 {
		return nil
	}
	return removedTableNodes(diff.TablesRemoved)
}
func removedTableNodes(names []string) []ast.Node {
	var nodes []ast.Node
	for _, name := range names {
		nodes = append(nodes, ast.NewDropTable(name))
	}
	return nodes
}

func changedColumnTTLPolicies(changes []difftypes.TableDiff, resets map[string]bool, policies map[string]*ast.YDBTieredTTLSpec) {
	for _, change := range changes {
		column := change.YDBColumnTableChange
		if column == nil || column.Current == nil || column.Desired == nil || ydbcolumn.TTLEqual(column.Current.TTL, column.Desired.TTL) {
			continue
		}
		if column.Current.TTL != nil {
			resets[change.TableName] = true
		}
		if column.Desired.TTL != nil {
			policies[change.TableName] = column.Desired.TTL
		}
	}
}
