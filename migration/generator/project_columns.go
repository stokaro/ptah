package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
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
	converted, err := goschematodb.Columns(ctx, shape, fields, dialect, runtime)
	if err != nil {
		return nil, err
	}
	if len(converted) != len(fields) {
		return nil, fmt.Errorf("cannot project incomplete columns of %q", table.TableName)
	}
	definitions, err := columnDefinitions(ctx, table, converted, dialect, semantics, runtime)
	if err != nil {
		return nil, err
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

// columnDefinitions keys the converted definitions of the changed columns by
// column identity. An owner that predicts a read of its columns says what a
// changed column reads back as, and that prediction replaces the conversion:
// a conversion of the declaration alone keeps the declared spelling, such as
// BIGINT where YDB reports Int64, and the reverse plan then compares the
// prior column with a type no read reports. The conversion's facets stay.
func columnDefinitions(
	ctx context.Context, table difftypes.TableDiff, converted []catalog.Column, dialect string, semantics identifier.Semantics, runtime Runtime,
) (map[string]catalog.Column, error) {
	predicted, owned, err := goschematodb.PredictedColumns(ctx, table.Desired, dialect, runtime)
	if err != nil {
		return nil, err
	}
	definitions := make(map[string]catalog.Column, len(converted))
	for _, column := range converted {
		key := semantics.ColumnIdentityKey(column.Name)
		if _, duplicate := definitions[key]; duplicate {
			return nil, fmt.Errorf("duplicate projected column %q", column.Name)
		}
		if owned {
			at := slices.IndexFunc(predicted, func(read catalog.Column) bool { return semantics.ColumnIdentityKey(read.Name) == key })
			if at < 0 {
				return nil, fmt.Errorf("cannot project column %q the owner did not predict", column.Name)
			}
			facets := column.Facets
			column = predicted[at].Clone()
			column.Facets = facets
		}
		definitions[key] = column
	}
	return definitions, nil
}

func columnPosition(columns []catalog.Column, name string, semantics identifier.Semantics) int {
	key := semantics.ColumnIdentityKey(name)
	return slices.IndexFunc(columns, func(column catalog.Column) bool { return semantics.ColumnIdentityKey(column.Name) == key })
}
