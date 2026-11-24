package yamlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/tableref"
)

// schemaTrimDocument declares a table in a schema, the objects it holds, and a
// grant on the schema. {S} is where a schema and a table's name are written,
// and {N} is where a space goes around a name that keeps its bytes.
const schemaTrimDocument = `
enums:
  "{N}status{N}": [on, off]
tables:
  orders:
    name: '{S}orders{S}'
    schema: '{S}app{S}'
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
    on_schema: '{S}app{S}'
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

// A bare schema or table name that begins or ends with white space is refused.
// A document is text an author typed, so the reader removes that space, and
// every layer below keeps a name's bytes: `name: " t"` beside a live " t"
// planned CREATE TABLE t and DROP TABLE " t", and the apply lost the rows of
// " t". A table entry with schema "app " beside a live app.orders planned a
// DROP of app.orders.
func TestParse_FailurePathRefusesASpacedBareSchemaOrTableName(t *testing.T) {
	tests := []struct {
		name, document, wantErr string
	}{
		{
			name:     "a table name with a space before it",
			document: "tables:\n  spaced:\n    name: ' t'\n",
			wantErr:  `table "spaced": table name " t" begins or ends with white space, which a bare name loses: write it as one quoted identifier, " t", or remove the space`,
		},
		{
			name:     "a table name with a tab after it",
			document: "tables:\n  spaced:\n    name: \"t\\t\"\n",
			wantErr:  `table "spaced": table name "t\\t" begins or ends with white space, .*`,
		},
		{
			name:     "a table key with a space before it",
			document: "tables:\n  ' t':\n    columns: {}\n",
			wantErr:  `table " t": table name " t" begins or ends with white space, .*`,
		},
		{
			name:     "a qualified name with a space around it",
			document: "tables:\n  dotted:\n    name: ' metrics.hourly '\n",
			wantErr:  `table "dotted": table name " metrics.hourly " begins or ends with white space, which a bare name loses: quote the part that holds the space to keep it, or remove the space`,
		},
		{
			name:     "a bracket is no quote",
			document: "tables:\n  bracketed:\n    name: ' [t]'\n",
			wantErr:  `table "bracketed": table name " \[t\]" begins or ends with white space, .*`,
		},
		{
			name:     "a schema with a space after it",
			document: "tables:\n  orders:\n    schema: 'app '\n",
			wantErr:  `table "orders": schema name "app " begins or ends with white space, which a bare name loses: write it as one quoted identifier, "app ", or remove the space`,
		},
		{
			name:     "a grant's schema",
			document: "grants:\n  reader_app:\n    role: reader\n    privileges: [USAGE]\n    on_schema: ' app'\n",
			wantErr:  `grant "reader_app" on_schema: schema name " app" begins or ends with white space, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(c.Context(), yamlext.None(), []byte(test.document))

			c.Assert(err, qt.ErrorIs, tableref.ErrSpacedBareName)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// A schema written as one quoted identifier in double quotes is the name
// inside the quotes, bytes and all, so a document names a live "app " beside a
// live app. The space around the quotes is removed.
func TestParse_ReadsAQuotedSchemaWithItsBytes(t *testing.T) {
	c := qt.New(t)

	db := schemaTrimModel(c, `"`, "")

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "app")

	document := strings.NewReplacer("'{S}app{S}'", `' "app " '`, "'{S}orders{S}'", "orders", "{N}", "").Replace(schemaTrimDocument)
	quoted, err := yamlschema.Parse(c.Context(), yamlext.None(), []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(quoted.Tables, qt.HasLen, 1)
	c.Assert(quoted.Tables[0].Schema, qt.Equals, "app ")
	c.Assert(quoted.Grants, qt.HasLen, 1)
	c.Assert(quoted.Grants[0].OnSchema, qt.Equals, "app ")
}

// An object's name keeps the bytes the document writes. The catalog holds
// " note" and "note" as two columns, so a column, an index, a constraint, an
// enum or a function named with a space names the live object of that name:
// trimmed, a constraint named "ck " planned a DROP of "ck", which the server
// refused as absent.
func TestParse_KeepsTheSpaceAroundAnObjectName(t *testing.T) {
	c := qt.New(t)

	db := schemaTrimModel(c, "", " ")

	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[0].Name, qt.Equals, " id ")
	c.Assert(db.Fields[1].Name, qt.Equals, " email ")
	c.Assert(db.Fields[1].Default, qt.Equals, "'a '")
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, " orders_email ")
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].Name, qt.Equals, " email_set ")
	c.Assert(db.Enums, qt.HasLen, 1)
	c.Assert(db.Enums[0].Name, qt.Equals, " status ")
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Name, qt.Equals, " touch ")
}

// A table's name written as one quoted identifier in double quotes is the
// name inside the quotes, bytes and all, as the Go tag reader reads
// name="\" t\"". It is how a document names a live " t" beside a live t, and
// the name in the key reads as the name in a name field. Brackets and
// backticks are no quotes here: they are part of the name.
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
		{name: "bare with a dot", document: "tables:\n  dotted:\n    name: metrics.hourly\n", want: "metrics.hourly"},
		{name: "in brackets", document: "tables:\n  bracketed:\n    name: '[orders]'\n", want: "[orders]"},
		{name: "in backticks", document: "tables:\n  ticked:\n    name: '`orders`'\n", want: "`orders`"},
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
