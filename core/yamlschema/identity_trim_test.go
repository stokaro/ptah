package yamlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// schemaTrimDocument declares a table in a schema, the objects it holds, and a
// grant on the schema. {S} is where a space goes around a value the reader
// trims, a schema or a table's name, and {N} is where one goes around a name
// that keeps its bytes.
const schemaTrimDocument = `
enums:
  "{N}status{N}": [on, off]
tables:
  orders:
    name: "{S}orders{S}"
    schema: "{S}app{S}"
    columns:
      id:
        name: "{N}id{N}"
        type: INTEGER
        primary: true
      email:
        name: "{N}email{N}"
        type: TEXT
        default: "'a '"
    indexes:
      by_email:
        name: "{N}orders_email{N}"
        fields: [email]
    constraints:
      email_set:
        name: "{N}email_set{N}"
        type: CHECK
        check: "email <> ''"
functions:
  touch:
    name: "{N}touch{N}"
    returns: trigger
    language: plpgsql
    body: "BEGIN RETURN NEW; END;"
roles:
  reader:
    name: reader
grants:
  reader_app:
    role: reader
    privileges: [USAGE]
    on_schema: "{S}app{S}"
`

// schemaTrimModel parses the document with schema around every schema it marks
// and name around every object name it marks.
func schemaTrimModel(c *qt.C, schema, name string) *schemamodel.Database {
	c.Helper()
	document := strings.NewReplacer("{S}", schema, "{N}", name).Replace(schemaTrimDocument)
	db, err := yamlschema.Parse(c.Context(), yamlext.None(), []byte(document))
	c.Assert(err, qt.IsNil)
	return db
}

// A document is text an author typed, so the space around a schema is removed
// where the document is read, as it is around a table's name: it parses to the
// model the same document without the space does. Every layer below keeps a schema name's bytes, so a
// table entry with schema "app " beside a live app.orders planned a DROP of
// app.orders.
func TestParse_TrimsTheSpaceAroundASchemaField(t *testing.T) {
	for _, around := range []string{" ", "\t", "  "} {
		t.Run(strings.ReplaceAll(around, "\t", `\t`), func(t *testing.T) {
			c := qt.New(t)
			want := schemaTrimModel(c, "", "")

			got := schemaTrimModel(c, around, "")

			c.Assert(got.Tables, qt.DeepEquals, want.Tables)
			c.Assert(got.Fields, qt.DeepEquals, want.Fields)
			c.Assert(got.Indexes, qt.DeepEquals, want.Indexes)
			c.Assert(got.Constraints, qt.DeepEquals, want.Constraints)
			c.Assert(got.Grants, qt.DeepEquals, want.Grants)
		})
	}
}

// An object's name keeps the bytes the document writes. The catalog holds
// " note" and "note" as two columns, so a column, an index, a constraint, an
// enum or a function named with a space names the live object of that name:
// trimmed, a constraint named "ck " planned a DROP of "ck", which the server
// refused as absent.
func TestParse_KeepsTheSpaceAroundAnObjectName(t *testing.T) {
	c := qt.New(t)

	db := schemaTrimModel(c, " ", " ")

	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[0].Name, qt.Equals, " id ")
	c.Assert(db.Fields[1].Name, qt.Equals, " email ")
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, " orders_email ")
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].Name, qt.Equals, " email_set ")
	c.Assert(db.Enums, qt.HasLen, 1)
	c.Assert(db.Enums[0].Name, qt.Equals, " status ")
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Name, qt.Equals, " touch ")
}

// The controls: the spaced document holds the schemas trimmed, so the first
// test cannot pass on a parse that drops them, and a value that names no
// schema keeps its space.
func TestParse_KeepsTheNamesOfTheTrimDocument(t *testing.T) {
	c := qt.New(t)

	db := schemaTrimModel(c, " ", "")

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "app")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[1].Default, qt.Equals, "'a '")
}

// A table's name written as one quoted identifier is the name inside the
// quotes, bytes and all, as the Go tag reader reads name="\" t\"". It is how a
// document names a live " t" beside a live t; the bare spelling is trimmed to
// t, and the name in the key reads as the name in a name field.
func TestParse_ReadsAQuotedTableNameWithItsBytes(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{name: "quoted", document: "tables:\n  spaced:\n    name: '\" t\"'\n", want: " t"},
		{name: "quoted with space around the quotes", document: "tables:\n  spaced:\n    name: ' \"t \" '\n", want: "t "},
		{name: "quoted in the key", document: "tables:\n  '\" t\"':\n    columns: {}\n", want: " t"},
		{name: "quoted with a dot inside", document: "tables:\n  dotted:\n    name: '\"a.b\"'\n", want: "a.b"},
		{name: "bare with a space around it", document: "tables:\n  spaced:\n    name: ' t '\n", want: "t"},
		{name: "bare and qualified", document: "tables:\n  dotted:\n    name: metrics.hourly\n", want: "metrics.hourly"},
		{name: "bare and qualified with a space around it", document: "tables:\n  dotted:\n    name: ' metrics.hourly '\n", want: "metrics.hourly"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(c.Context(), yamlext.None(), []byte(test.document))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Name, qt.Equals, test.want)
			c.Assert(db.Tables[0].Schema, qt.Equals, "")
		})
	}
}
