package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// relativeTimeSchema declares column a of table t as field, beside a key.
func relativeTimeSchema(field schemamodel.Field) *schemamodel.Database {
	field.StructName, field.FieldName, field.Name, field.Nullable = "T", "A", "a", true
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			field,
		},
	}
}

// A default a Go or YAML model declares, or an ALTER COLUMN ... SET DEFAULT
// sets, is refused on a date or time column where the target reads it as the
// moment the table is created, as a CREATE TABLE in a schema file is.
func TestGetOrderedCreateStatements_ARelativeTimeDefaultIsRefused_FailurePath(t *testing.T) {
	const tail = ` column is read once, when the table is created, so every row gets that moment and every plan ` +
		`changes the column again; write a function the server calls for each row, such as now\(\) or CURRENT_DATE`
	tests := []struct {
		name, dialect string
		field         schemamodel.Field
		wantErr       string
	}{
		{
			name: "a literal now", dialect: platform.Postgres,
			field:   schemamodel.Field{Type: "TIMESTAMPTZ", Default: "now", DefaultSet: true},
			wantErr: `column "a" of table "t": DEFAULT 'now' on a TIMESTAMPTZ` + tail,
		},
		{
			name: "a quoted literal today", dialect: platform.Postgres,
			field:   schemamodel.Field{Type: "date", Default: "'Today'", DefaultSet: true},
			wantErr: `column "a" of table "t": DEFAULT 'Today' on a date` + tail,
		},
		{
			name: "an expression in parentheses", dialect: platform.Postgres,
			field:   schemamodel.Field{Type: "timestamptz", DefaultExpr: "('now')"},
			wantErr: `column "a" of table "t": DEFAULT \('now'\) on a timestamptz` + tail,
		},
		{
			name: "YugabyteDB", dialect: platform.YugabyteDB,
			field:   schemamodel.Field{Type: "timestamptz", DefaultExpr: "'now'::timestamptz"},
			wantErr: `column "a" of table "t": DEFAULT 'now'::timestamptz on a timestamptz` + tail,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(relativeTimeSchema(test.field), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A fixed moment, a function, the word on a text column, and the word on an
// engine that keeps it as written all render.
func TestGetOrderedCreateStatements_ARelativeTimeDefaultIsRefused_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		field         schemamodel.Field
	}{
		{name: "a fixed moment", dialect: platform.Postgres, field: schemamodel.Field{Type: "timestamptz", Default: "epoch", DefaultSet: true}},
		{name: "a function", dialect: platform.Postgres, field: schemamodel.Field{Type: "timestamptz", DefaultExpr: "now()"}},
		{name: "a text column", dialect: platform.Postgres, field: schemamodel.Field{Type: "text", Default: "now", DefaultSet: true}},
		{name: "CockroachDB", dialect: platform.CockroachDB, field: schemamodel.Field{Type: "timestamptz", Default: "now", DefaultSet: true}},
		{name: "MySQL", dialect: platform.MySQL, field: schemamodel.Field{Type: "datetime", DefaultExpr: "'now'"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(relativeTimeSchema(test.field), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
		})
	}
}

// Each input a schema is read from reaches the refusal: a Go struct tag, a
// YAML column, and a SQL schema file that sets the default with ALTER COLUMN
// ... SET DEFAULT after a CREATE TABLE that declares none.
func TestGetOrderedCreateStatements_ARelativeTimeDefaultFromEachInputIsRefused(t *testing.T) {
	const want = `column "at" of table "(public\.)?events": DEFAULT \(?'now'\)? on a (TIMESTAMPTZ|timestamptz) column is read once, .*`
	t.Run("a Go struct", func(t *testing.T) {
		c := qt.New(t)
		database, err := goschema.ParseSource(annotation.None(), "events.go", relativeTimeGoSource)
		c.Assert(err, qt.IsNil)

		statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, want)
		c.Assert(statements, qt.IsNil)
	})
	t.Run("a YAML schema", func(t *testing.T) {
		c := qt.New(t)
		database, err := yamlschema.Parse(t.Context(), yamlext.None(), []byte(relativeTimeYAMLSource))
		c.Assert(err, qt.IsNil)

		statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, want)
		c.Assert(statements, qt.IsNil)
	})
	t.Run("ALTER COLUMN SET DEFAULT in a SQL schema file", func(t *testing.T) {
		c := qt.New(t)
		database, _, err := sqlschema.Read([]byte(relativeTimeAlterSource), platform.Postgres)
		c.Assert(err, qt.IsNil)

		statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, want)
		c.Assert(statements, qt.IsNil)
	})
}

const relativeTimeGoSource = `package events

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="at" type="TIMESTAMPTZ" default="now"
	At string
}
`

const relativeTimeYAMLSource = "tables:\n  events:\n    columns:\n      id: {type: INT, primary: true}\n      at: {type: TIMESTAMPTZ, default: now}\n"

const relativeTimeAlterSource = "CREATE TABLE events (id int PRIMARY KEY, at timestamptz);\n" +
	"ALTER TABLE events ALTER COLUMN at SET DEFAULT ('now');\n"
