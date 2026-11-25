package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// schemaTrimSource declares one object of every directive that names a schema,
// and the objects a table holds. {S} is where a schema is written, and a
// table's name, and {N} is where a space goes around a name that keeps its
// bytes.
const schemaTrimSource = `
package fixture

//ptah:schema:schema name="{S}app{S}"
type App struct{}

//ptah:schema:table name="{S}orders{S}" schema="{S}app{S}"
type Order struct {
	//ptah:schema:field name="{N}id{N}" type="INTEGER" primary="true"
	ID int

	//ptah:schema:field name="{N}email{N}" type="TEXT" default="'a '"
	Email string

	//ptah:schema:index name="{N}orders_email{N}" fields="email"
	_ int
}

//ptah:schema:sequence name="{N}counter{N}" schema="{S}app{S}"
type Counter struct{}

//ptah:schema:view name="v_orders" schema="{S}app{S}" body="SELECT 1"
type OrderView struct{}

//ptah:schema:function name="{N}touch{N}" returns="trigger" language="plpgsql" body="BEGIN RETURN NEW; END;"
type Touch struct{}

//ptah:schema:data table="orders" schema="{S}app{S}" key="id" file="orders.yaml"
type OrderRows struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}

//ptah:schema:grant role="reader" privileges="USAGE" on_schema="{S}app{S}"
type ReaderGrant struct{}
`

// schemaTrimFixture is the source with schema around every schema it marks and
// name around every object name it marks.
func schemaTrimFixture(schema, name string) string {
	return strings.NewReplacer("{S}", schema, "{N}", name).Replace(schemaTrimSource)
}

// A bare schema or table name that begins or ends with white space is refused.
// A tag is text an author typed, so the reader removes that space, and every
// layer below keeps a name's bytes: name=" t" beside a live " t" planned a
// CREATE TABLE t and a DROP TABLE " t", and the apply lost the rows of " t".
// A sequence tagged schema="app " beside a live app.counter would plan in app
// as if the author had written app.
func TestParseSource_FailurePathRefusesASpacedBareSchemaOrTableName(t *testing.T) {
	tests := []struct {
		name, annotation, wantErr string
	}{
		{
			name:       "a table name with a space before it",
			annotation: `//ptah:schema:table name=" t"`,
			wantErr:    `name " t" begins or ends with white space, which a bare name loses: write it as one quoted identifier, " t", or remove the space on //ptah:schema:table at S`,
		},
		{
			name:       "a table name with a tab after it",
			annotation: "//ptah:schema:table name=\"t\t\"",
			wantErr:    `name "t\\t" begins or ends with white space, .*`,
		},
		{
			name:       "a table name with a space after the dot",
			annotation: `//ptah:schema:table name="public. t"`,
			wantErr:    `name "public. t" begins or ends with white space, which a bare name loses: quote the part that holds the space to keep it, or remove the space`,
		},
		{
			name:       "a schema attribute with a space after it",
			annotation: `//ptah:schema:table name="t" schema="app "`,
			wantErr:    `schema attribute "app " begins or ends with white space, which a bare name loses: write it as one quoted identifier, "app ", or remove the space at fixture.go:3`,
		},
		{
			name:       "a schema directive",
			annotation: `//ptah:schema:schema name=" app"`,
			wantErr:    `name attribute " app" begins or ends with white space, .* at fixture.go:3`,
		},
		{
			name:       "a sequence's schema",
			annotation: `//ptah:schema:sequence name="counter" schema=" app"`,
			wantErr:    `schema attribute " app" begins or ends with white space, .* at fixture.go:3`,
		},
		{
			name:       "a grant's schema",
			annotation: `//ptah:schema:grant role="reader" privileges="USAGE" on_schema="app "`,
			wantErr:    `on_schema attribute "app " begins or ends with white space, .* at fixture.go:3`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(noOwners, "fixture.go",
				"package m\n\n"+test.annotation+"\ntype S struct{}\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorIs, tableref.ErrSpacedBareName)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr+`.*`)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// A schema or a table name written as one quoted identifier is the name inside
// the quotes, bytes and all: it is how a tag names a live " t" beside a live t,
// or the schema "app " beside app.
func TestParseSource_ReadsAQuotedSchemaOrTableNameWithItsBytes(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", schemaTrimFixture(`\"`, ""))

	c.Assert(db.Schemas, qt.HasLen, 1)
	c.Assert(db.Schemas[0].Name, qt.Equals, "app")
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")

	spaced := mustParseSource(c, "schema.go", strings.NewReplacer(`"{S}app{S}"`, `"\" app\""`,
		`"{S}orders{S}"`, `"\"orders \""`, "{N}", "").Replace(schemaTrimSource))

	c.Assert(spaced.Schemas, qt.HasLen, 1)
	c.Assert(spaced.Schemas[0].Name, qt.Equals, " app")
	c.Assert(spaced.Tables, qt.HasLen, 1)
	c.Assert(spaced.Tables[0].Name, qt.Equals, "orders ")
	c.Assert(spaced.Tables[0].Schema, qt.Equals, " app")
	c.Assert(spaced.Sequences, qt.HasLen, 1)
	c.Assert(spaced.Sequences[0].Schema, qt.Equals, " app")
	c.Assert(spaced.Views, qt.HasLen, 1)
	c.Assert(spaced.Views[0].Name, qt.Equals, `" app".v_orders`)
	c.Assert(spaced.ManagedData, qt.HasLen, 1)
	c.Assert(spaced.ManagedData[0].Schema, qt.Equals, " app")
	c.Assert(spaced.Grants, qt.HasLen, 1)
	c.Assert(spaced.Grants[0].OnSchema, qt.Equals, " app")
}

// An object's name keeps the bytes its tag writes. The catalog holds " note"
// and "note" as two columns, so a column, an index or a function named with a
// space names the live object of that name: trimmed, field name=" note" beside
// a live " note" planned an ADD of "note" and a DROP of " note", and the apply
// lost the column's values.
func TestParseSource_KeepsTheSpaceAroundAnObjectName(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", schemaTrimFixture("", " "))

	c.Assert(fieldNames(db.Fields), qt.DeepEquals, []string{" id ", " email ", "id"})
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, " orders_email ")
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Name, qt.Equals, " touch ")
	// A sequence's name is trimmed as the model reads it.
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "counter")
	c.Assert(db.Fields, qt.HasLen, 3)
	c.Assert(db.Fields[1].Default, qt.Equals, "'a '")
}

// fieldNames lists the name of each field, in order.
func fieldNames(fields []schemamodel.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}
