package sqlitekey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/sqlitekey"
)

// TestImpliesNotNull is the shape table measured with `pragma table_info` on
// SQLite 3.51.0, one row per shape, plus the two rows that are not key columns at
// all. Each `want` is the notnull the catalog reports for that DDL, so a row that
// disagrees with SQLite is a failing row rather than a redefinition of the rule.
func TestImpliesNotNull(t *testing.T) {
	tests := []struct {
		name       string
		table      goschema.Table
		keyColumns []string
		field      goschema.Field
		want       bool
	}{
		{
			name:       "rowid table text key is nullable",
			table:      goschema.Table{Name: "t"},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "TEXT", Primary: true},
			want:       false,
		},
		{
			name:       "rowid table integer key is nullable",
			table:      goschema.Table{Name: "t"},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INTEGER", Primary: true},
			want:       false,
		},
		{
			name:       "rowid table composite key is nullable",
			table:      goschema.Table{Name: "t", PrimaryKey: []string{"a", "b"}},
			keyColumns: []string{"a", "b"},
			field:      goschema.Field{Name: "a", Type: "TEXT"},
			want:       false,
		},
		{
			name:       "without rowid text key is not null",
			table:      goschema.Table{Name: "t", WithoutRowID: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "TEXT", Primary: true},
			want:       true,
		},
		{
			name:       "without rowid integer key is not null",
			table:      goschema.Table{Name: "t", WithoutRowID: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INTEGER", Primary: true},
			want:       true,
		},
		{
			name:       "without rowid composite key is not null",
			table:      goschema.Table{Name: "t", WithoutRowID: true, PrimaryKey: []string{"a", "b"}},
			keyColumns: []string{"a", "b"},
			field:      goschema.Field{Name: "b", Type: "TEXT"},
			want:       true,
		},
		{
			name:       "strict text key is not null",
			table:      goschema.Table{Name: "t", Strict: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "TEXT", Primary: true},
			want:       true,
		},
		{
			name:       "strict integer key is the rowid alias and stays nullable",
			table:      goschema.Table{Name: "t", Strict: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INTEGER", Primary: true},
			want:       false,
		},
		{
			name:       "strict INT key is not the rowid alias",
			table:      goschema.Table{Name: "t", Strict: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INT", Primary: true},
			want:       true,
		},
		{
			name:       "strict composite key is not null",
			table:      goschema.Table{Name: "t", Strict: true, PrimaryKey: []string{"a", "b"}},
			keyColumns: []string{"a", "b"},
			field:      goschema.Field{Name: "a", Type: "TEXT"},
			want:       true,
		},
		{
			// SQLite would answer notnull=1 here, because DESC defeats the
			// rowid alias. Ptah answers 0 on purpose: its SQLite renderer drops
			// DESC from a PRIMARY KEY, so the table it actually builds from this
			// schema does have the alias, and answering SQLite's DESC rule would
			// plan a rebuild against that table on every run. Measured:
			// `PRIMARY KEY (id DESC)` in a STRICT source is applied as
			// `PRIMARY KEY ("id")` and the catalog reports notnull=0.
			name: "strict integer key ordered DESC follows what the renderer builds",
			table: goschema.Table{
				Name:            "t",
				Strict:          true,
				PrimaryKeyParts: []goschema.PrimaryKeyPart{{Name: "id", Desc: true}},
			},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INTEGER"},
			want:       false,
		},
		{
			name:       "strict and without rowid together answer not null",
			table:      goschema.Table{Name: "t", Strict: true, WithoutRowID: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "id", Type: "INTEGER", Primary: true},
			want:       true,
		},
		{
			name:       "a column outside the key is never touched",
			table:      goschema.Table{Name: "t", Strict: true, WithoutRowID: true},
			keyColumns: []string{"id"},
			field:      goschema.Field{Name: "note", Type: "TEXT", Nullable: true},
			want:       false,
		},
		{
			name:       "a table with no key at all",
			table:      goschema.Table{Name: "t", Strict: true},
			keyColumns: nil,
			field:      goschema.Field{Name: "note", Type: "TEXT", Nullable: true},
			want:       false,
		},
		{
			name:       "key columns are matched the way SQLite compares names",
			table:      goschema.Table{Name: "t", WithoutRowID: true},
			keyColumns: []string{"ID"},
			field:      goschema.Field{Name: "id", Type: "TEXT", Primary: true},
			want:       true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlitekey.ImpliesNotNull(test.table, test.keyColumns, test.field), qt.Equals, test.want)
		})
	}
}

// TestKeyColumns pins where the key is read from, because "is this column in the
// key" and "is the key one column" are both answers the nullability rule needs.
func TestKeyColumns(t *testing.T) {
	tests := []struct {
		name   string
		table  goschema.Table
		fields []goschema.Field
		want   []string
	}{
		{
			name:   "no key",
			table:  goschema.Table{Name: "t"},
			fields: []goschema.Field{{Name: "a"}},
			want:   nil,
		},
		{
			name:   "field level key",
			table:  goschema.Table{Name: "t"},
			fields: []goschema.Field{{Name: "a"}, {Name: "id", Primary: true}},
			want:   []string{"id"},
		},
		{
			name:   "two field level key columns are a composite key",
			table:  goschema.Table{Name: "t"},
			fields: []goschema.Field{{Name: "a", Primary: true}, {Name: "b", Primary: true}},
			want:   []string{"a", "b"},
		},
		{
			name:   "table level key",
			table:  goschema.Table{Name: "t", PrimaryKey: []string{"a", "b"}},
			fields: []goschema.Field{{Name: "a"}, {Name: "b"}},
			want:   []string{"a", "b"},
		},
		{
			name: "table level key parts win over the plain name list",
			table: goschema.Table{
				Name:            "t",
				PrimaryKey:      []string{"ignored"},
				PrimaryKeyParts: []goschema.PrimaryKeyPart{{Name: "a"}, {Name: "b", Desc: true}},
			},
			fields: []goschema.Field{{Name: "a"}, {Name: "b"}},
			want:   []string{"a", "b"},
		},
		{
			name:   "an empty name in the key list is not a column",
			table:  goschema.Table{Name: "t", PrimaryKey: []string{"a", "  "}},
			fields: []goschema.Field{{Name: "a"}},
			want:   []string{"a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlitekey.KeyColumns(test.table, test.fields), qt.DeepEquals, test.want)
		})
	}
}
