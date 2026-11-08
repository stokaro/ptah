package goschematodb

import (
	"context"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/convert/features"
	"ptah.run/internal/schemaprep"
)

// Columns converts accepted column definitions without predicting a new table.
// Table supplies common key context only. Existing-table changes must not apply
// CREATE defaults or consume unrelated table facets. Column facets still pass
// through their selected conversion owners. Errors return no partial columns.
func Columns(ctx context.Context, table schemamodel.Table, fields []schemamodel.Field, target string, runtime interface {
	schemaext.ConversionRuntime
	schemaext.TargetResolver
	schemaprojection.ReadBackRuntime
}) ([]catalog.Column, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return nil, err
	}
	readBack, err := runtime.ReadBack(selected.Name())
	if err != nil {
		return nil, err
	}
	columns := toDBColumns(table, fields, selected.Name(), readBack)
	projected := &catalog.Database{Tables: []catalog.Table{{Columns: columns}}}
	applyTablePrimaryKeys(projected, []schemamodel.Table{table})
	_, _, err = features.Convert(ctx, runtime, selected.Name(), schemaext.Desired, schemaext.Observed, schemaext.Objects{}, schemaext.Coverage{}, projected.FacetSlots())
	if err != nil {
		return nil, err
	}
	return projected.Tables[0].Columns, nil
}

// PredictedColumns is what the owner of target predicts a read of a table
// created from declaration reports for its columns, in declaration order, and
// false where the owner predicts no columns, as a target without an owned
// column service does. A projection of an existing table's changed columns
// takes a changed column's observation from it, because only the owner knows
// what a read reports for a declared type: YDB stores a column declared BIGINT
// as Int64. It is a prediction from the declaration, never inspection
// evidence, and it carries no facets. Errors return no columns.
func PredictedColumns(ctx context.Context, declaration schemacapture.TableDeclaration, target string, runtime Runtime) ([]catalog.Column, bool, error) {
	if !declaration.HasTable() {
		return nil, false, nil
	}
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{declaration.Table.Clone()}, Fields: slices.Clone(declaration.Fields), Enums: slices.Clone(declaration.Enums),
		Constraints: slices.Clone(declaration.Constraints), FeatureObjects: declaration.OwnedObjects, FeatureCoverage: declaration.FeatureCoverage,
	}
	_, prepared, err := prepareSourceTables(ctx, source, target, runtime)
	if err != nil {
		return nil, false, err
	}
	if len(prepared) != 1 || !prepared[0].ColumnsPrepared {
		return nil, false, nil
	}
	columns := make([]catalog.Column, len(prepared[0].Columns))
	for i, column := range prepared[0].Columns {
		columns[i] = column.Clone()
	}
	return columns, true, nil
}

// ColumnConstraints converts the CHECK and the foreign key that each named
// column of declaration states on itself, in declaration order. They belong
// to the column rather than to its table: the comparison leaves them to the
// column's lifecycle, so a change that adds the column adds them with it and
// lists none of them among its constraint additions. A CHECK takes the name
// [schemaprep.ColumnCheckNames] answers on target, the name the comparison
// pairs it by, and a foreign key its declared name or
// [schemaprep.GenerateForeignKeyName]. A name declaration does not hold
// converts nothing.
func ColumnConstraints(declaration schemacapture.TableDeclaration, columns []string, target string) []catalog.Constraint {
	table := declaration.Table
	checkNames := schemaprep.ColumnCheckNames(table, declaration.Fields, declaration.Constraints, target)
	var constraints []catalog.Constraint
	for _, field := range declaration.Fields {
		if field.StructName != table.StructName || !slices.Contains(columns, field.Name) {
			continue
		}
		constraints = append(constraints, toDBFieldConstraints(table, schemaprep.WithJSONCheck(field, target), checkNames[field.Name])...)
	}
	return constraints
}
