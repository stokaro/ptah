package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff/difftypes"
)

// projectColumns applies accepted column changes to the captured table. The
// declaration supplies each changed definition, but cannot remove an omitted
// column unless the accepted diff contains that removal.
func projectColumns(ctx context.Context, table difftypes.TableDiff, dialect string, semantics identifier.Semantics, runtime Runtime) ([]catalog.Column, error) {
	columns := slices.Clone(table.Current.Table.Columns)
	for i := range columns {
		columns[i] = columns[i].Clone()
	}
	if len(table.ColumnsAdded)+len(table.ColumnsModified)+len(table.ColumnsRemoved) == 0 {
		return columns, nil
	}
	if !table.Current.HasTable() || !table.Desired.HasTable() {
		return nil, fmt.Errorf("cannot project columns of %q without captured table operands", table.TableName)
	}
	// Conversion is restricted to the accepted field definitions. Table
	// properties, sibling indexes, and unrelated feature values are not inputs
	// to a column's state transition.
	shape := schemamodel.Table{Name: table.Desired.Table.Name, Schema: table.Desired.Table.Schema,
		StructName: table.Desired.Table.StructName, PrimaryKey: slices.Clone(table.Desired.Table.PrimaryKey)}
	fields := slices.Clone([]schemamodel.Field(table.ColumnsAdded))
	for _, change := range table.ColumnsModified {
		fields = append(fields, change.Desired)
	}
	converted, err := goschematodb.ToDBSchema(ctx, &schemamodel.Database{
		Tables: []schemamodel.Table{shape}, Fields: fields, Enums: table.Desired.Enums,
	}, dialect, runtime)
	if err != nil {
		return nil, err
	}
	if len(converted.Tables) != 1 || len(converted.Tables[0].Columns) != len(fields) {
		return nil, fmt.Errorf("cannot project incomplete columns of %q", table.TableName)
	}
	definitions := make(map[string]catalog.Column, len(fields))
	for _, column := range converted.Tables[0].Columns {
		key := semantics.ColumnIdentityKey(column.Name)
		if _, duplicate := definitions[key]; duplicate {
			return nil, fmt.Errorf("duplicate projected column %q", column.Name)
		}
		definitions[key] = column
	}
	for _, removed := range table.ColumnsRemoved {
		position := columnPosition(columns, removed.Name, semantics)
		if position < 0 {
			return nil, fmt.Errorf("cannot project removal of missing column %q", removed.Name)
		}
		columns = slices.Delete(columns, position, position+1)
	}
	for _, changed := range table.ColumnsModified {
		position := columnPosition(columns, changed.ColumnName, semantics)
		definition, found := definitions[semantics.ColumnIdentityKey(changed.ColumnName)]
		if position < 0 || !found {
			return nil, fmt.Errorf("cannot project modification of missing column %q", changed.ColumnName)
		}
		columns[position] = definition
	}
	for _, added := range table.ColumnsAdded {
		definition, found := definitions[semantics.ColumnIdentityKey(added.Name)]
		if !found || columnPosition(columns, added.Name, semantics) >= 0 {
			return nil, fmt.Errorf("cannot project conflicting addition of column %q", added.Name)
		}
		columns = append(columns, definition)
	}
	for i := range columns {
		columns[i].OrdinalPosition = i + 1
	}
	return columns, nil
}

func columnPosition(columns []catalog.Column, name string, semantics identifier.Semantics) int {
	key := semantics.ColumnIdentityKey(name)
	return slices.IndexFunc(columns, func(column catalog.Column) bool { return semantics.ColumnIdentityKey(column.Name) == key })
}
