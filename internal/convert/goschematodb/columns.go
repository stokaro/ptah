package goschematodb

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/features"
)

// Columns converts accepted column definitions without predicting a new table.
// Table supplies common key context only. Existing-table changes must not apply
// CREATE defaults or consume unrelated table facets. Column facets still pass
// through their selected conversion owners. Errors return no partial columns.
func Columns(ctx context.Context, table schemamodel.Table, fields []schemamodel.Field, target string, runtime interface {
	schemaext.ConversionRuntime
	schemaext.TargetResolver
}) ([]catalog.Column, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return nil, err
	}
	columns := toDBColumns(table, fields, selected.Name())
	projected := &catalog.Database{Tables: []catalog.Table{{Columns: columns}}}
	applyTablePrimaryKeys(projected, []schemamodel.Table{table})
	_, _, err = features.Convert(ctx, runtime, selected.Name(), schemaext.Desired, schemaext.Observed, schemaext.Objects{}, schemaext.Coverage{}, projected.FacetSlots())
	if err != nil {
		return nil, err
	}
	return projected.Tables[0].Columns, nil
}
