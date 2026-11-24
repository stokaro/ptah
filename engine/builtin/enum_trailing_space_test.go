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
	"ptah.run/core/schemamodel"
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

// TestRenderEnumTrailingSpace_HappyPath renders the enum values a server
// keeps: a leading space and a trailing tab on the MySQL family, which removes
// only trailing spaces, and a trailing space on PostgreSQL, which keeps it.
func TestRenderEnumTrailingSpace_HappyPath(t *testing.T) {
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
			name:    "a trailing space on PostgreSQL",
			dialect: platform.Postgres,
			database: enumColumnDatabase(schemamodel.Field{Type: "mood"},
				schemamodel.Enum{Name: "mood", Values: []string{"happy", "ok "}}),
			want: `'ok '`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
				test.database, test.dialect, capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestRenderEnumTrailingSpace_FailurePath refuses an enum value that ends in
// a space on the MySQL family, in each form a schema states the values in.
// The server would store the value without the space, so the column would
// never match its declaration.
func TestRenderEnumTrailingSpace_FailurePath(t *testing.T) {
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
			name:     "a value of spaces alone",
			dialect:  platform.MySQL,
			database: enumColumnDatabase(schemamodel.Field{Type: "ENUM", Enum: []string{"happy", "  "}}),
			want: `column "n" of table "c" declares the enum value "  ", and mysql removes the trailing spaces ` +
				`of an ENUM value, so the column would never match its declaration: declare a value that does not end in a space`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
				test.database, test.dialect, capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.want)+`.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// An Atlas HCL document states the values in the column's type, and the
// refusal reads them there.
func TestRenderEnumTrailingSpace_FailurePathReadsAnHCLDocument(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	parsed, err := atlashcl.ParseWithOptions(t.Context(), []byte(`schema "s" {}
table "c" {
  schema = schema.s
  column "id" {
    type = int
  }
  column "n" {
    type = enum("happy", " sad", "ok ")
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
		parsed, platform.MySQL, capability.ForDialect(platform.MySQL),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*declares the enum value "ok ", and mysql removes the trailing spaces `+
		`of an ENUM value, so the column would never match its declaration: declare "ok".*`)
	c.Assert(statements, qt.IsNil)
}
