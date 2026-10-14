package embedydb

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/embedgen"
)

// describe reads one row table's columns, key, indexes and attributes. exists
// is false when nothing holds the path.
func (b *Backend) describe(ctx context.Context, schema, table string) (ydbschema.RowTable, bool, error) {
	session, err := b.db.Conn(ctx)
	if err != nil {
		return ydbschema.RowTable{}, false, fmt.Errorf("open a YDB session: %w", err)
	}
	defer func() { _ = session.Close() }()
	return ydbschema.DescribeRowTable(ctx, session, directory(schema), strings.TrimSpace(table))
}

// keyColumn is one column of a source key, with the type a key value read as
// text is cast back to.
type keyColumn struct {
	name     string
	ydbType  string
	nullable bool
}

// cast renders a parameter cast to the column's type. A key travels as text,
// because that is the one rendering every key type has; YQL compares typed
// values only, so the text is cast back before it meets the column, which also
// keeps the comparison on the primary key the read is ordered by.
func (k keyColumn) cast(parameter int) string {
	return fmt.Sprintf("CAST($p%d AS %s)", parameter, k.ydbType)
}

// sourceKey reads a source table's key and refuses a specification whose key
// fields are not the table's primary key.
//
// The keyset scan resumes after the last key it read, and the write path
// addresses a row by its key. On YDB both are only exact through the primary
// key: it is what orders a table, so a page is a range read, and it is what
// identifies a row, so an UPDATE by it touches one row. Key fields that are
// anything else would make a scan a full read per page and a write a search.
func (b *Backend) sourceKey(ctx context.Context, spec embedgen.Spec) ([]keyColumn, error) {
	described, exists, err := b.describe(ctx, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("source table %s does not exist", tablePath(spec.Source.Schema, spec.Source.Table))
	}
	return keyOf(described, spec.Source.KeyFields, tablePath(spec.Source.Schema, spec.Source.Table))
}

// keyOf matches key fields against a described table's primary key.
func keyOf(described ydbschema.RowTable, fields []string, name string) ([]keyColumn, error) {
	if !slices.Equal(described.PrimaryKey, fields) {
		return nil, fmt.Errorf("%w: key_fields %s are not the primary key of %s, which is %s. On YDB a "+
			"generation reads and writes a table by its primary key, so key_fields name it, in key order",
			ErrUnsupported, quotedList(fields), name, quotedList(described.PrimaryKey))
	}
	key := make([]keyColumn, 0, len(fields))
	for _, field := range fields {
		column, found := described.Column(field)
		if !found {
			return nil, fmt.Errorf("table %s has no column %s", name, field)
		}
		key = append(key, keyColumn{name: field, ydbType: column.Type, nullable: column.Nullable})
	}
	return key, nil
}

// quotedList renders names for a diagnostic.
func quotedList(names []string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = strconv.Quote(name)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// keyMatch renders the predicate that addresses one key: each column equal to
// its parameter cast back to the column's type, numbered from first.
func keyMatch(alias string, key []keyColumn, first int) string {
	conditions := make([]string, len(key))
	for index, column := range key {
		conditions[index] = qualified(alias, column.name) + " = " + column.cast(first+index)
	}
	return strings.Join(conditions, " AND ")
}

// qualified renders a column under an alias, or bare when there is none.
func qualified(alias, column string) string {
	if alias == "" {
		return quote(column)
	}
	return alias + "." + quote(column)
}

// asText renders a column read as text, which is how a key, an input field
// and a version travel.
func asText(alias, column string) string {
	return "CAST(" + qualified(alias, column) + " AS Utf8)"
}
