package builtin_test

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
)

// enumColumnDatabase is the table c whose column n is field, beside the
// declared enums.
func enumColumnDatabase(field schemamodel.Field, enums ...schemamodel.Enum) *schemamodel.Database {
	field.Name, field.StructName = "n", "C"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "c", StructName: "C"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "C", Type: "INTEGER", Primary: true}, field},
		Enums:  enums,
	}
}

// binaryCharset is the column settings that give a column the binary
// character set.
var binaryCharset = must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "binary"}))

// TestValidateEnumTrailingSpace_HappyPath accepts the enum values a server
// keeps: a leading space and a trailing tab on the MySQL family, which removes
// only trailing spaces, a trailing space on PostgreSQL, which keeps it, and a
// trailing space in a MySQL-family column of the binary character set, which
// keeps it too. Measured on MySQL 8.4 and MariaDB 11.8, ENUM('ok ','x')
// CHARACTER SET binary stores 6F6B20 for 'ok '. The schema then renders with
// the value.
func TestValidateEnumTrailingSpace_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		database      *schemamodel.Database
		want          string
	}{
		{
			name:     "a leading space on MySQL",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", " sad"}}),
			want:     `' sad'`,
		},
		{
			name:     "a trailing tab on MariaDB",
			dialect:  platform.MariaDB,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", "ok\t"}}),
			want:     "'ok\t'",
		},
		{
			name:     "a SET member with a leading space on MySQL",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "SET('a',' b')"}),
			want:     "SET('a',' b')",
		},
		{
			name:    "a trailing space on PostgreSQL",
			dialect: platform.Postgres,
			database: enumColumnDatabase(schemamodel.Field{Type: "mood"},
				schemamodel.Enum{Name: "mood", Values: []string{"happy", "ok "}}),
			want: `'ok '`,
		},
		{
			name:     "a column of the binary character set",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"ok ", "x"}, Facets: binaryCharset}),
			want:     `'ok '`,
		},
		{
			name:     "a SET column of the binary collation",
			dialect:  platform.MariaDB,
			database: enumColumnDatabase(schemamodel.Field{Type: "SET('m ','n')", Collate: "binary"}),
			want:     `SET('m ','n')`,
		},
		{
			name:     "the binary character set in the type text",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM('ok ', 'x') CHARACTER SET binary"}),
			want:     `'ok '`,
		},
		{
			name:     "the binary collation in the type text",
			dialect:  platform.MariaDB,
			database: enumColumnDatabase(schemamodel.Field{Type: "SET('m ', 'n') COLLATE binary"}),
			want:     `'m '`,
		},
		{
			name:     "a table of the binary collation",
			dialect:  platform.MySQL,
			database: binaryDefaultDatabase(schemamodel.Table{Collate: "binary"}, schemamodel.Schema{}),
			want:     `'ok '`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForDialect(test.dialect)

			err := builtin.ValidateSchemaWithCapabilities(test.database, test.dialect, caps)

			c.Assert(err, qt.IsNil)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.database, test.dialect, caps)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// binaryDefaultDatabase is the table c, with the defaults table states, in
// the schema, whose column n declares the value 'ok ' and names no character
// set or collation of its own.
func binaryDefaultDatabase(table schemamodel.Table, schema schemamodel.Schema) *schemamodel.Database {
	database := enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"ok ", "x"}})
	table.Name, table.StructName = "c", "C"
	database.Tables = []schemamodel.Table{table}
	if schema.Name != "" {
		database.Schemas = []schemamodel.Schema{schema}
	}
	return database
}

// TestRenderEnumTrailingSpace_HappyPath renders a value that ends in a space
// in a column that names no character set, which validation refuses. A read
// verb renders what a server holds, and a column of the binary character set
// holds such a value: refused in the render, `db read` and `schema inspect
// --format sql` could not describe that database.
func TestRenderEnumTrailingSpace_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		field         schemamodel.Field
		want          string
	}{
		{name: "an ENUM value on MySQL", dialect: platform.MySQL, field: schemamodel.Field{Type: "enum('ok ','x')"}, want: `enum('ok ','x')`},
		{name: "a SET member on MariaDB", dialect: platform.MariaDB, field: schemamodel.Field{Type: "set('m ','n')"}, want: `set('m ','n')`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
				enumColumnDatabase(test.field), test.dialect, capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestValidateEnumTrailingSpace_FailurePath refuses an enum value that ends in
// a space on the MySQL family, in each form a schema states the values in.
// The server would store the value without the space, so the column would
// never match its declaration.
func TestValidateEnumTrailingSpace_FailurePath(t *testing.T) {
	const okOnMySQL = `column "n" of table "c" declares the enum value "ok ", and mysql removes the trailing spaces ` +
		`of an ENUM value, so the column would never match its declaration: declare "ok"`
	tests := []struct {
		name, dialect string
		database      *schemamodel.Database
		want          string
	}{
		{
			name:     "values on MySQL",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", " sad", "ok "}}),
			want:     okOnMySQL,
		},
		{
			name:     "values on MariaDB",
			dialect:  platform.MariaDB,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", "ok "}}),
			want: `column "n" of table "c" declares the enum value "ok ", and mariadb removes the trailing spaces ` +
				`of an ENUM value, so the column would never match its declaration: declare "ok"`,
		},
		{
			name:     "the type text of an HCL column",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: `enum("happy","ok ")`}),
			want:     okOnMySQL,
		},
		{
			name:    "a declared enum",
			dialect: platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "mood"},
				schemamodel.Enum{Name: "mood", Values: []string{"happy", "ok "}}),
			want: okOnMySQL,
		},
		{
			name:     "the members of a SET type on MySQL",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "SET('a ','b')"}),
			want: `column "n" of table "c" declares the SET member "a ", and mysql removes the trailing spaces ` +
				`of a SET member, so the column would never match its declaration: declare "a"`,
		},
		{
			name:     "the members of an HCL set type on MariaDB",
			dialect:  platform.MariaDB,
			database: enumColumnDatabase(schemamodel.Field{Type: `set("a", "b ")`}),
			want: `column "n" of table "c" declares the SET member "b ", and mariadb removes the trailing spaces ` +
				`of a SET member, so the column would never match its declaration: declare "b"`,
		},
		{
			name:     "a value of spaces alone",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", "  "}}),
			want: `column "n" of table "c" declares the enum value "  ", and mysql removes the trailing spaces ` +
				`of an ENUM value, so the column would never match its declaration: declare a value that does not end in a space`,
		},
		{
			// Measured on MySQL 8.4 and MariaDB 11.8: utf8mb4, its _bin and
			// _0900_bin collations, latin1_bin and ucs2 all remove the space.
			name:     "a character set after the list",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM('ok ', 'x') CHARACTER SET utf8mb4"}),
			want:     okOnMySQL,
		},
		{
			name:     "a collation after the list",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM('ok ', 'x') COLLATE utf8mb4_bin"}),
			want:     okOnMySQL,
		},
		{
			name:     "a column of another character set in a binary table",
			dialect:  platform.MySQL,
			database: binaryTableColumn(schemamodel.Field{Type: "ENUM('ok ', 'x') CHARSET latin1"}),
			want:     okOnMySQL,
		},
		{
			name:     "a column of another collation in a binary table",
			dialect:  platform.MySQL,
			database: binaryTableColumn(schemamodel.Field{Type: "ENUM", Enum: []string{"ok ", "x"}, Collate: "utf8mb4_bin"}),
			want:     okOnMySQL,
		},
		{
			name:    "a utf8mb4 table in a binary schema",
			dialect: platform.MySQL,
			database: binaryDefaultDatabase(schemamodel.Table{Schema: "s", Collate: "utf8mb4_bin"},
				schemamodel.Schema{Name: "s", Charset: "binary", Collate: "binary"}),
			want: `column "n" of table "s.c" declares the enum value "ok "`,
		},
		{
			// A plan creates the table in an existing schema without changing
			// its default (stokaro/ptah#6665), so a binary schema default does
			// not say what the column takes.
			name:    "a column in a binary schema",
			dialect: platform.MariaDB,
			database: binaryDefaultDatabase(schemamodel.Table{Schema: "s"},
				schemamodel.Schema{Name: "s", Charset: "binary", Collate: "binary"}),
			want: `column "n" of table "s.c" declares the enum value "ok "`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchemaWithCapabilities(test.database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.want)+`.*`)
		})
	}
}

// binaryTableColumn is the table c of the binary collation, whose column n is
// field.
func binaryTableColumn(field schemamodel.Field) *schemamodel.Database {
	database := enumColumnDatabase(field)
	database.Tables[0].Collate = "binary"
	return database
}

// parsedEnumDocument parses the Atlas HCL document of the table c, in the
// schema s, with the table attributes and the column n.
func parsedEnumDocument(c *qt.C, tableAttributes, column string) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	parsed, err := atlashcl.ParseWithOptions(c.Context(), []byte(`schema "s" {}
table "c" {
  schema = schema.s
`+tableAttributes+`
  column "id" {
    type = int
  }
  column "n" {
`+column+`
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	return parsed
}

// An Atlas HCL document states the values in the column's type, and the
// refusal reads them there.
func TestValidateEnumTrailingSpace_FailurePathReadsAnHCLDocument(t *testing.T) {
	c := qt.New(t)
	parsed := parsedEnumDocument(c, "", `    type = enum("happy", " sad", "ok ")`)

	err := builtin.ValidateSchemaWithCapabilities(parsed, platform.MySQL, capability.ForDialect(platform.MySQL))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*declares the enum value "ok ", and mysql removes the trailing spaces `+
		`of an ENUM value, so the column would never match its declaration: declare "ok".*`)
}

// An Atlas HCL document gives a column or its table the binary character set
// with the charset attribute, and the column keeps the space.
func TestValidateEnumTrailingSpace_HappyPathReadsAnHCLDocument(t *testing.T) {
	tests := []struct {
		name, table, column string
	}{
		{name: "the column's charset", column: `    type = enum("ok ", "x")
    charset = "binary"`},
		{name: "the table's charset", table: `  charset = "binary"`, column: `    type = enum("ok ", "x")`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed := parsedEnumDocument(c, test.table, test.column)

			err := builtin.ValidateSchemaWithCapabilities(parsed, platform.MariaDB, capability.ForDialect(platform.MariaDB))

			c.Assert(err, qt.IsNil)
		})
	}
}
