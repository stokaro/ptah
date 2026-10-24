package datadiff

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
)

// RenderContext is [Render] through [RenderStatementsContext]: the scripts it
// returns are those statements joined with "\n".
func RenderContext(
	ctx context.Context,
	diff *DataDiff,
	dialect string,
	services dataext.Services,
	caps capability.Capabilities,
) (up, down string, err error) {
	upStmts, downStmts, err := RenderStatementsContext(ctx, diff, dialect, services, caps)
	if err != nil {
		return "", "", err
	}
	return joinStatements(upStmts), joinStatements(downStmts), nil
}

// RenderStatementsContext renders diff as [RenderStatements] does, for a
// target whose capabilities are caps, and through the owner's write service
// where services carries one (dataext.Services.Writes).
//
// Without a write service it is [RenderStatements], and ctx and caps are not
// read. With one, the owner spells every statement: the up and down writes are
// the operations RenderStatements renders, in the same order -- an insert of
// every column of the row, sorted; an update by key that sets the columns
// outside the key, sorted; a delete by key -- and are sent in one batch, up
// first, so the owner sees the whole diff at once. diff.ColumnTypes reaches the
// owner as each write's column types. ctx bounds the service. A batch the owner
// refuses is an error and no statement.
func RenderStatementsContext(
	ctx context.Context,
	diff *DataDiff,
	dialect string,
	services dataext.Services,
	caps capability.Capabilities,
) (up, down []string, err error) {
	if services.Writes == nil {
		return RenderStatements(diff, dialect, services)
	}
	if diff == nil {
		return nil, nil, errors.New("datadiff: nil diff")
	}
	if len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletes) == 0 {
		return nil, nil, nil
	}
	if len(diff.Keys) == 0 {
		return nil, nil, errors.New("datadiff: keys must be non-empty to render a non-empty diff")
	}
	upWrites, err := diff.upWrites()
	if err != nil {
		return nil, nil, err
	}
	downWrites, err := diff.downWrites()
	if err != nil {
		return nil, nil, err
	}
	statements, err := services.Writes.RenderWrites(ctx, caps, slices.Concat(upWrites, downWrites))
	if err != nil {
		return nil, nil, fmt.Errorf("datadiff: render the row writes of %s: %w", diff.Table, err)
	}
	if len(statements) != len(upWrites)+len(downWrites) {
		return nil, nil, fmt.Errorf("datadiff: the owner rendered %d statements for %d row writes", len(statements), len(upWrites)+len(downWrites))
	}
	return statements[:len(upWrites):len(upWrites)], statements[len(upWrites):], nil
}

// upWrites returns the row writes of the up direction, in the order
// [renderUp] writes its statements.
func (d *DataDiff) upWrites() ([]dataext.RowWrite, error) {
	var writes rowWrites
	for _, row := range d.Inserts {
		if err := writes.add(d.insert(row)); err != nil {
			return nil, err
		}
	}
	for _, update := range d.Updates {
		if err := writes.add(d.update(update.Desired, update.Key)); err != nil {
			return nil, err
		}
	}
	for _, row := range d.Deletes {
		if err := writes.add(d.delete(row)); err != nil {
			return nil, err
		}
	}
	return writes, nil
}

// downWrites returns the row writes of the down direction, in the order
// [renderDown] writes its statements.
func (d *DataDiff) downWrites() ([]dataext.RowWrite, error) {
	var writes rowWrites
	for _, row := range slices.Backward(d.Deletes) {
		if err := writes.add(d.insert(row)); err != nil {
			return nil, err
		}
	}
	for _, update := range slices.Backward(d.Updates) {
		if err := writes.add(d.update(update.Live, update.Key)); err != nil {
			return nil, err
		}
	}
	for _, row := range slices.Backward(d.Inserts) {
		if err := writes.add(d.delete(row)); err != nil {
			return nil, err
		}
	}
	return writes, nil
}

type rowWrites []dataext.RowWrite

func (w *rowWrites) add(write dataext.RowWrite, err error) error {
	if err != nil {
		return err
	}
	*w = append(*w, write)
	return nil
}

func (d *DataDiff) write(mode dataext.WriteMode, columns []string, values []any) dataext.RowWrite {
	types := make(map[string]string, len(columns))
	for _, column := range columns {
		if declared, ok := d.ColumnTypes[column]; ok && declared != "" {
			types[column] = declared
		}
	}
	return dataext.RowWrite{Mode: mode, Schema: d.Schema, Table: d.Table, Columns: columns,
		Keys: slices.Sorted(slices.Values(d.Keys)), Values: values, ColumnTypes: types}
}

func (d *DataDiff) insert(row Row) (dataext.RowWrite, error) {
	columns := sortedColumns(row)
	if len(columns) == 0 {
		return dataext.RowWrite{}, errors.New("datadiff: cannot render INSERT for a row with no columns")
	}
	values := make([]any, len(columns))
	for i, column := range columns {
		values[i] = row[column]
	}
	return d.write(dataext.WriteInsert, columns, values), nil
}

// update writes the key columns, valued from keyRow, and the columns of
// setRow outside the key, sorted.
func (d *DataDiff) update(setRow, keyRow Row) (dataext.RowWrite, error) {
	keys, values, err := d.keyValues(keyRow)
	if err != nil {
		return dataext.RowWrite{}, err
	}
	columns := keys
	for _, column := range sortedColumns(setRow) {
		if !slices.Contains(d.Keys, column) {
			columns, values = append(columns, column), append(values, setRow[column])
		}
	}
	if len(columns) == len(keys) {
		return dataext.RowWrite{}, errors.New("datadiff: cannot render UPDATE with no non-key columns to set")
	}
	return d.write(dataext.WriteUpdateByKey, columns, values), nil
}

func (d *DataDiff) delete(row Row) (dataext.RowWrite, error) {
	keys, values, err := d.keyValues(row)
	if err != nil {
		return dataext.RowWrite{}, err
	}
	return d.write(dataext.WriteDeleteByKey, keys, values), nil
}

// keyValues returns the key columns, sorted, and their values in row.
func (d *DataDiff) keyValues(row Row) ([]string, []any, error) {
	keys := slices.Sorted(slices.Values(d.Keys))
	values := make([]any, len(keys))
	for i, key := range keys {
		value, ok := row[key]
		if !ok {
			return nil, nil, fmt.Errorf("datadiff: missing key column %q", key)
		}
		values[i] = value
	}
	return keys, values, nil
}
