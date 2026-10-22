package providerhost

import (
	"context"
	"fmt"

	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
)

type commonPlanner struct {
	options       planext.Options
	target        string
	assessParents bool
	transactions  bool
	commonTables  bool
}

func (p commonPlanner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diff == nil {
		return nil, fmt.Errorf("provider planner requires a diff")
	}
	if !p.commonTables && ((len(diff.TablesAdded) > 0 && !p.assessParents) || len(diff.TablesRemoved) > 0 || len(diff.TablesModified) > 0) {
		return nil, unsupported("common table planning without " + provider.CommonTableIdentityFeature)
	}
	if err := onlyFields(p.options, "TableRebuildRequest", "Capabilities"); err != nil {
		return nil, err
	}
	if err := onlyFields(*diff, "TablePreparation", "IdentifierSemantics", "TablesAdded", "TablesRemoved", "TablesModified",
		"DeclaredTables", "DeclaredSchemas", "DeclaredConstraintHosts", "ObservedConstraintHosts",
		"DeclaredTableDependencies", "Features", "FeatureChanges", "CurrentDatabasePath"); err != nil {
		return nil, err
	}
	nodes := make([]ast.Node, 0)
	var columnRemovals []ast.Node
	ordered := diff.TablesAdded.InDependencyOrder()
	if len(ordered) != len(diff.TablesAdded) {
		return nil, fmt.Errorf("provider planner received incomplete or duplicate table creations")
	}
	for _, creation := range ordered {
		node, err := p.creationNode(creation)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	for _, table := range diff.TablesModified {
		additions, removals, err := commonAlterations(table)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, additions...)
		columnRemovals = append(columnRemovals, removals...)
	}
	removalStart := len(nodes)
	nodes = append(nodes, columnRemovals...)
	for _, removal := range diff.TablesRemoved {
		if !removal.Current.HasTable() {
			return nil, fmt.Errorf("provider planner requires captured table state")
		}
	}
	for _, name := range deporder.TableDropOrderWithDependencies(diff.TablesRemoved.Names(), diff.DeclaredTables, diff.DeclaredTableDependencies) {
		nodes = append(nodes, &ast.DropTableNode{Name: name})
	}
	return p.schedule(ctx, runtime, diff, nodes, removalStart)
}

func commonAlterations(table difftypes.TableDiff) (nodes, removals []ast.Node, err error) {
	if err := onlyFields(table, "TableName", "ColumnsAdded", "ColumnsRemoved", "Desired", "Current", "ColumnKeyNames", "FeatureChanges"); err != nil {
		return nil, nil, err
	}
	if !table.Desired.HasTable() || !table.Current.HasTable() {
		return nil, nil, fmt.Errorf("provider planner requires both captured table states")
	}
	name := tableref.CanonicalExact(table.Desired.Table.Schema, table.Desired.Table.Name)
	nodes = make([]ast.Node, 0, len(table.ColumnsAdded))
	for _, field := range table.ColumnsRemoved {
		removals = append(removals, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: field.Name}}})
	}
	for _, field := range table.ColumnsAdded {
		column, err := commonColumn(field)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}})
	}
	return nodes, removals, nil
}
