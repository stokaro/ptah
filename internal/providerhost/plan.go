package providerhost

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/schemacapture"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
)

type commonPlanner struct{ options planext.Options }

func (p commonPlanner) GenerateMigrationAST(ctx context.Context, _ featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diff == nil {
		return nil, fmt.Errorf("provider planner requires a diff")
	}
	if err := onlyFields(p.options, "TableRebuildRequest"); err != nil {
		return nil, err
	}
	if err := onlyFields(*diff, "TablePreparation", "IdentifierSemantics", "TablesAdded", "TablesRemoved", "TablesModified",
		"DeclaredTables", "DeclaredSchemas", "DeclaredConstraintHosts", "ObservedConstraintHosts",
		"DeclaredTableDependencies", "Features", "CurrentDatabasePath"); err != nil {
		return nil, err
	}
	nodes := make([]ast.Node, 0)
	for _, removal := range diff.TablesRemoved {
		if err := commonObservation(removal.Current); err != nil {
			return nil, err
		}
	}
	for _, name := range deporder.TableDropOrderWithDependencies(diff.TablesRemoved.Names(), diff.DeclaredTables, diff.DeclaredTableDependencies) {
		nodes = append(nodes, &ast.DropTableNode{Name: name})
	}
	ordered := diff.TablesAdded.InDependencyOrder()
	if len(ordered) != len(diff.TablesAdded) {
		return nil, fmt.Errorf("provider planner received incomplete or duplicate table creations")
	}
	for _, creation := range ordered {
		if err := onlyFields(creation, "Name", "Table", "Fields", "FeatureCoverage", "DependsOn"); err != nil {
			return nil, err
		}
		node, err := commonTable(schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields})
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	for _, table := range diff.TablesModified {
		additions, err := commonAlterations(table)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, additions...)
	}
	return nodes, nil
}

func commonAlterations(table difftypes.TableDiff) ([]ast.Node, error) {
	if err := onlyFields(table, "TableName", "ColumnsAdded", "ColumnsRemoved", "Desired", "Current", "ColumnKeyNames"); err != nil {
		return nil, err
	}
	if _, err := commonTable(table.Desired); err != nil {
		return nil, err
	}
	if err := commonObservation(table.Current); err != nil {
		return nil, err
	}
	name := tableref.CanonicalExact(table.Desired.Table.Schema, table.Desired.Table.Name)
	nodes := make([]ast.Node, 0, len(table.ColumnsAdded)+len(table.ColumnsRemoved))
	for _, field := range table.ColumnsRemoved {
		nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: field.Name}}})
	}
	for _, field := range table.ColumnsAdded {
		column, err := commonColumn(field)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}})
	}
	return nodes, nil
}

func commonObservation(observed schemacapture.TableObservation) error {
	if err := onlyFields(observed, "Table", "FeatureCoverage"); err != nil {
		return err
	}
	if observed.Table.Name == "" {
		return fmt.Errorf("provider planner requires captured table state")
	}
	if err := onlyFields(observed.Table, "Name", "Schema", "Type", "Columns", "EstimatedRows", "RowStatsUnknown"); err != nil {
		return err
	}
	for _, column := range observed.Table.Columns {
		if err := commonObservedColumn(column); err != nil {
			return err
		}
	}
	return nil
}

func commonObservedColumn(column catalog.Column) error {
	return onlyFields(column, "Name", "DataType", "FormattedType", "IsNullable", "IsPrimaryKey", "ColumnDefault", "OrdinalPosition", "TypeIsDeclaredText")
}
