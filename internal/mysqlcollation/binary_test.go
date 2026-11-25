package mysqlcollation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlcollation"
)

// columnCharset is the owner's setting that names charset for a column.
func columnCharset(charset string) schemaext.Facets {
	return must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: charset}))
}

// tableCharset is a table whose owner options name charset.
func tableCharset(charset string) schemamodel.Table {
	return schemamodel.Table{Name: "c", Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Charset: charset}))}
}

// TestBinary_HappyPath reads which character set decides a column: what the
// column names, in its type text, its owner setting or its collation, before
// the table's defaults.
func TestBinary_HappyPath(t *testing.T) {
	enum := schemamodel.Field{Name: "n", Type: "ENUM('ok ','x')"}
	tests := []struct {
		name  string
		table schemamodel.Table
		field schemamodel.Field
		want  bool
	}{
		{name: "a column that names nothing in a table that names nothing", field: enum},
		{name: "the type text", field: schemamodel.Field{Type: "enum('ok ') CHARACTER SET binary"}, want: true},
		{name: "a collation in the type text", field: schemamodel.Field{Type: "set('m ') COLLATE binary"}, want: true},
		{name: "the owner's setting", field: schemamodel.Field{Type: "ENUM", Facets: columnCharset("binary")}, want: true},
		{name: "the column's collation", field: schemamodel.Field{Type: "ENUM", Collate: "binary"}, want: true},
		{name: "a column of another character set", field: schemamodel.Field{Type: "ENUM", Facets: columnCharset("utf8mb4")}},
		{name: "the table's collation", table: schemamodel.Table{Collate: "binary"}, field: enum, want: true},
		{name: "the table's character set", table: tableCharset("binary"), field: enum, want: true},
		{
			name:  "the table's platform group",
			table: schemamodel.Table{Overrides: map[string]map[string]string{platform.MariaDB: {"charset": "binary"}}},
			field: enum, want: true,
		},
		{
			// The column decides before the table.
			name:  "a column of another character set in a binary table",
			table: schemamodel.Table{Collate: "binary"},
			field: schemamodel.Field{Type: "enum('ok ') CHARACTER SET latin1"},
		},
		{
			name:  "a column of another collation in a binary table",
			table: tableCharset("binary"),
			field: schemamodel.Field{Type: "ENUM", Collate: "utf8mb4_bin"},
		},
		{
			name:  "a binary column in a utf8mb4 table",
			table: tableCharset("utf8mb4"),
			field: schemamodel.Field{Type: "ENUM", Facets: columnCharset("BINARY")},
			want:  true,
		},
		{
			// The character set decides where both are named.
			name:  "a character set beside the binary collation",
			field: schemamodel.Field{Type: "enum('ok ') CHARACTER SET utf8mb4", Collate: "binary"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := mysqlcollation.Binary(platform.MariaDB, test.table, test.field)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// mismatchedSettings claims the MySQL column settings kind with a type the
// owner does not define.
type mismatchedSettings struct{}

func (*mismatchedSettings) Kind() schemaext.Kind   { return mysqlschema.ColumnSettingsKind }
func (*mismatchedSettings) Clone() schemaext.Value { return &mismatchedSettings{} }
func (*mismatchedSettings) Equal(other schemaext.Value) bool {
	_, ok := other.(*mismatchedSettings)
	return ok
}

// A column settings value of another type is refused rather than read as
// naming nothing.
func TestBinary_FailurePath(t *testing.T) {
	c := qt.New(t)
	field := schemamodel.Field{Type: "ENUM", Facets: must.Must(schemaext.NewFacets(&mismatchedSettings{}))}

	got, err := mysqlcollation.Binary(platform.MySQL, schemamodel.Table{Collate: "binary"}, field)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.IsFalse)
}
