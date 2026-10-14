package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/dialect/ydb/ydbschema"
)

// RowTable is one row table as [DescribeRowTable] reads it: what a caller
// writing into the table needs to know about its columns, its key and its
// indexes, without reading the whole schema.
type RowTable struct {
	// Columns are the table's columns, in the order the table declares them.
	Columns []RowColumn
	// PrimaryKey names the key columns, in key order.
	PrimaryKey []string
	// Indexes are the table's indexes, by name.
	Indexes []RowIndex
	// Attributes are the table's user attributes, which is where YDB keeps a
	// comment Ptah wrote.
	Attributes map[string]string
}

// RowColumn is one column of a row table.
type RowColumn struct {
	// Name is the column's name.
	Name string
	// Type is the column's type as YQL spells it, without Optional.
	Type string
	// Nullable reports whether the type is Optional.
	Nullable bool
}

// RowIndex is one index of a row table.
type RowIndex struct {
	// Name is the index's name.
	Name string
	// Columns are the indexed columns, in index order.
	Columns []string
	// Vector is a vector index's settings, and nil for every other kind.
	Vector *ydbschema.VectorSettings
	// Ready reports that the index finished building: YDB answers a query
	// through an index only once its build has completed.
	Ready bool
}

// Column returns the named column, and false when the table has none.
func (t RowTable) Column(name string) (RowColumn, bool) {
	index := slices.IndexFunc(t.Columns, func(column RowColumn) bool { return column.Name == name })
	if index < 0 {
		return RowColumn{}, false
	}
	return t.Columns[index], true
}

// Index returns the named index, and false when the table has none.
func (t RowTable) Index(name string) (RowIndex, bool) {
	index := slices.IndexFunc(t.Indexes, func(candidate RowIndex) bool { return candidate.Name == name })
	if index < 0 {
		return RowIndex{}, false
	}
	return t.Indexes[index], true
}

// DescribeRowTable describes the row table name in the directory schema,
// relative to the root of the database session is connected to, with "" for
// the root itself.
//
// It reads one path through the scheme and table services, where reading the
// schema would describe every object in the database. exists is false when
// nothing holds the name, including when its directory does not exist; an
// object of another kind under the name is an error. A vector index is decoded
// from the field the pinned protocol buffers do not model, as the reader
// decodes it; an index of another kind is reported with its name and columns
// and no vector settings.
func DescribeRowTable(ctx context.Context, session *sql.Conn, schema, name string) (RowTable, bool, error) {
	var bound conn
	if err := session.Raw(func(raw any) error {
		typed, ok := raw.(conn)
		if !ok || typed.driver == nil {
			return fmt.Errorf("%T is not a connection to a YDB database Ptah opened", raw)
		}
		bound = typed
		return nil
	}); err != nil {
		return RowTable{}, false, err
	}
	root := bound.root
	if root == "" {
		root = bound.driver.Name()
	}
	source, end, err := newGRPCSource(ctx, bound.driver)
	if err != nil {
		return RowTable{}, false, err
	}
	defer end()

	directory := path.Join("/"+strings.Trim(root, "/"), schema)
	_, entries, err := source.ListDirectory(ctx, directory)
	if isSchemeError(err) {
		return RowTable{}, false, nil
	}
	if err != nil {
		return RowTable{}, false, err
	}
	position := slices.IndexFunc(entries, func(entry *Ydb_Scheme.Entry) bool { return entry.GetName() == name })
	if position < 0 {
		return RowTable{}, false, nil
	}
	if entryType := entries[position].GetType(); entryType != Ydb_Scheme.Entry_TABLE {
		return RowTable{}, false, fmt.Errorf("YDB object %s is a %s, not a row table",
			path.Join(directory, name), entryTypeName(entryType))
	}
	described, err := source.DescribeTable(ctx, path.Join(directory, name))
	if err != nil {
		return RowTable{}, false, err
	}
	table, err := rowTable(described)
	if err != nil {
		return RowTable{}, false, fmt.Errorf("describe YDB table %s: %w", path.Join(directory, name), err)
	}
	return table, true, nil
}

// rowTable decodes a table description into a [RowTable].
func rowTable(described *Ydb_Table.DescribeTableResult) (RowTable, error) {
	table := RowTable{
		PrimaryKey: slices.Clone(described.GetPrimaryKey()),
		Attributes: maps.Clone(described.GetAttributes()),
	}
	for _, meta := range described.GetColumns() {
		ydbType, nullable, err := columnType(meta.GetType())
		if err != nil {
			return RowTable{}, fmt.Errorf("column %q: %w", meta.GetName(), err)
		}
		table.Columns = append(table.Columns, RowColumn{Name: meta.GetName(), Type: ydbType, Nullable: nullable})
	}
	for _, index := range described.GetIndexes() {
		decoded := RowIndex{
			Name:    index.GetName(),
			Columns: slices.Clone(index.GetIndexColumns()),
			Ready:   index.GetStatus() == Ydb_Table.TableIndexDescription_STATUS_READY,
		}
		if index.GetType() == nil {
			vector, isVector, err := vectorIndex(index)
			if err != nil {
				return RowTable{}, fmt.Errorf("index %q: %w", index.GetName(), err)
			}
			if isVector {
				decoded.Vector = vector
			}
		}
		table.Indexes = append(table.Indexes, decoded)
	}
	return table, nil
}
