package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// schemaTrimSource declares one object of every directive that names a schema,
// and the objects a table holds. {S} is where a space goes around a value the
// tag reader trims: a schema, a table's name, and the name of a view qualified
// by its schema. {N} is where one goes around a name that keeps its bytes.
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

//ptah:schema:view name="{S}v_orders{S}" schema="{S}app{S}" body="SELECT 1"
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

// A tag is text an author typed, so the space around a schema is removed where
// the tag is read, as it is around a table's name: the source parses to the
// model the same source without the space does. Every layer below keeps a schema name's bytes, so a schema read
// with its space names another schema, and a sequence tagged schema="app "
// beside a live app.counter planned a DROP of app.counter.
func TestParseSource_TrimsTheSpaceAroundASchemaAttribute(t *testing.T) {
	for _, around := range []string{" ", "\t", "  "} {
		t.Run(strings.ReplaceAll(around, "\t", `\t`), func(t *testing.T) {
			c := qt.New(t)
			want := mustParseSource(c, "schema.go", schemaTrimFixture("", ""))

			got := mustParseSource(c, "schema.go", schemaTrimFixture(around, ""))

			c.Assert(got.Schemas, qt.DeepEquals, want.Schemas)
			c.Assert(got.Tables, qt.DeepEquals, want.Tables)
			c.Assert(got.Fields, qt.DeepEquals, want.Fields)
			c.Assert(got.Sequences, qt.DeepEquals, want.Sequences)
			c.Assert(got.Views, qt.DeepEquals, want.Views)
			c.Assert(got.ManagedData, qt.DeepEquals, want.ManagedData)
			c.Assert(got.Grants, qt.DeepEquals, want.Grants)
		})
	}
}

// An object's name keeps the bytes its tag writes. The catalog holds " note"
// and "note" as two columns, so a column, an index or a function named with a
// space names the live object of that name: trimmed, field name=" note" beside
// a live " note" planned an ADD of "note" and a DROP of " note", and the apply
// lost the column's values.
func TestParseSource_KeepsTheSpaceAroundAnObjectName(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", schemaTrimFixture(" ", " "))

	c.Assert(fieldNames(db.Fields), qt.DeepEquals, []string{" id ", " email ", "id"})
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, " orders_email ")
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Name, qt.Equals, " touch ")
	// A sequence's name is trimmed as the model reads it, as a table's is.
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "counter")
}

// The controls: the spaced fixture holds the schemas trimmed, so the first test
// cannot pass on a parse that drops them, and a value that names no schema
// keeps its space.
func TestParseSource_KeepsTheNamesOfTheTrimFixture(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", schemaTrimFixture(" ", ""))

	c.Assert(db.Schemas, qt.HasLen, 1)
	c.Assert(db.Schemas[0].Name, qt.Equals, "app")
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Schema, qt.Equals, "app")
	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Name, qt.Equals, "app.v_orders")
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Schema, qt.Equals, "app")
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "app")
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
