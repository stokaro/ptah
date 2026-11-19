package builtin_test

import (
	"strings"
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
	"ptah.run/internal/builtintest"
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
		{name: "an undeclared type named update_kind", dialect: platform.Postgres, field: schemamodel.Field{Type: "update_kind", Default: "today", DefaultSet: true}},
		{name: "an undeclared type named timeslot", dialect: platform.Postgres, field: schemamodel.Field{Type: "timeslot", DefaultExpr: "'now'"}},
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

// A column typed by a domain the schema declares over a date or time type is
// refused, wherever the domain is declared and however many domains stand
// between the column and the built-in type.
func TestGetOrderedCreateStatements_ARelativeTimeDefaultOnADomainIsRefused(t *testing.T) {
	const tail = ` column is read once, when the table is created, .*`
	tests := []struct{ name, source, wantErr string }{
		{
			name:    "a domain over a timestamp",
			source:  "CREATE DOMAIN moment AS timestamp;\nCREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT 'now');\n",
			wantErr: `column "at" of table "(public\.)?events": DEFAULT 'now' on a (public\.)?moment` + tail,
		},
		{
			name: "a domain over a domain over a timestamptz",
			source: "CREATE DOMAIN instant AS timestamptz;\nCREATE DOMAIN moment AS instant;\n" +
				"CREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT ('now'));\n",
			wantErr: `column "at" of table "(public\.)?events": DEFAULT \('now'\) on a (public\.)?moment` + tail,
		},
		{
			name: "a domain declared after the table",
			source: "CREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT 'now');\n" +
				"CREATE DOMAIN moment AS timestamp(3) with time zone;\n",
			wantErr: `column "at" of table "(public\.)?events": DEFAULT 'now' on a (public\.)?moment` + tail,
		},
		{
			name: "a domain in the schema the column names",
			source: "CREATE SCHEMA app;\nCREATE DOMAIN app.day AS date;\nCREATE DOMAIN day AS text;\n" +
				"CREATE TABLE events (id int PRIMARY KEY, at app.day DEFAULT 'today');\n",
			wantErr: `column "at" of table "(public\.)?events": DEFAULT 'today' on a app\.day` + tail,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte(test.source), platform.Postgres, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)

			statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A domain a Go model declares over a timestamp is resolved the same way.
func TestGetOrderedCreateStatements_ARelativeTimeDefaultOnAGoDomainIsRefused(t *testing.T) {
	c := qt.New(t)
	database, err := goschema.ParseSource(builtintest.Annotations(), "events.go", relativeTimeGoDomainSource)
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `column "at" of table "(public\.)?events": DEFAULT 'now' on a (public\.)?moment column is read once, .*`)
	c.Assert(statements, qt.IsNil)
}

// A string default on a type that holds no moment renders, whatever the
// type's name says: an enum that has the value, a domain over text, a domain
// over such an enum, and a name two domains share, which the model cannot
// resolve.
func TestGetOrderedCreateStatements_AStringDefaultOnATypeThatHoldsNoMomentRenders(t *testing.T) {
	tests := []struct{ name, source string }{
		{
			name:   "an enum named update_kind",
			source: "CREATE TYPE update_kind AS ENUM ('today', 'later');\nCREATE TABLE events (id int PRIMARY KEY, kind update_kind DEFAULT 'today');\n",
		},
		{
			name:   "a domain over text",
			source: "CREATE DOMAIN moment AS text;\nCREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT 'now');\n",
		},
		{
			name: "a domain over an enum",
			source: "CREATE TYPE update_kind AS ENUM ('today', 'later');\nCREATE DOMAIN update_day AS update_kind;\n" +
				"CREATE TABLE events (id int PRIMARY KEY, kind update_day DEFAULT 'today');\n",
		},
		{
			name: "a name two domains share, the first over a timestamp",
			source: "CREATE SCHEMA a;\nCREATE SCHEMA b;\nCREATE DOMAIN a.moment AS timestamp;\nCREATE DOMAIN b.moment AS text;\n" +
				"CREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT 'now');\n",
		},
		{
			name: "a name two domains share, the second over a timestamp",
			source: "CREATE SCHEMA a;\nCREATE SCHEMA b;\nCREATE DOMAIN a.moment AS text;\nCREATE DOMAIN b.moment AS timestamp;\n" +
				"CREATE TABLE events (id int PRIMARY KEY, at moment DEFAULT 'now');\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte(test.source), platform.Postgres, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)

			statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
		})
	}
}

// An enum a Go model declares with the value today takes it as its default.
func TestGetOrderedCreateStatements_AnEnumDefaultNamedTodayRendersFromGo(t *testing.T) {
	c := qt.New(t)
	database, err := goschema.ParseSource(builtintest.Annotations(), "events.go", relativeTimeGoEnumSource)
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "DEFAULT 'today'")
}

const relativeTimeGoDomainSource = `package events

//ptah:schema:domain name="moment" type="TIMESTAMP"
type MomentDomain struct{}

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="at" type="moment" default="now"
	At string
}
`

const relativeTimeGoEnumSource = `package events

//ptah:schema:enum name="update_kind" values="today,later"
type UpdateKind struct{}

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="kind" type="update_kind" default="today"
	Kind string
}
`

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
