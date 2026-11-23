package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// identityTrimSource declares one object of every directive whose schema, name
// or table a plan keys on. Each %s is where a space goes around a value.
const identityTrimSource = `
package fixture

//ptah:schema:schema name="%sapp%s"
type App struct{}

//ptah:schema:table name="%sorders%s" schema="%sapp%s"
type Order struct {
	//ptah:schema:field name="%sid%s" type="INTEGER" primary="true"
	ID int

	//ptah:schema:field name="%semail%s" type="TEXT" default="'a '"
	Email string

	//ptah:schema:index name="%sorders_email%s" fields="email"
	_ int
}

//ptah:schema:sequence name="%scounter%s" schema="%sapp%s"
type Counter struct{}

//ptah:schema:enum name="%sstatus%s" values="on,off"
type Status string

//ptah:schema:view name="%sv_orders%s" schema="%sapp%s" body="SELECT 1"
type OrderView struct{}

//ptah:schema:data table="%sorders%s" schema="%sapp%s" key="id" file="orders.yaml"
type OrderRows struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}

//ptah:schema:grant role="reader" privileges="USAGE" on_schema="%sapp%s"
type ReaderGrant struct{}
`

// identityTrimFixture is the source with around before and after every
// value it marks.
func identityTrimFixture(around string) string {
	return strings.ReplaceAll(identityTrimSource, "%s", around)
}

// A tag is text an author typed, so the space around a schema, a name or a
// table is removed where the tag is read: the source parses to the model the
// same source without the space does. Every layer below keeps a name's bytes,
// so a value read with its space names another object, and a sequence tagged
// schema="app " beside a live app.counter planned a DROP of app.counter.
func TestParseSource_TrimsTheSpaceAroundAnIdentityAttribute(t *testing.T) {
	for _, around := range []string{" ", "\t", "  "} {
		t.Run(strings.ReplaceAll(around, "\t", `\t`), func(t *testing.T) {
			c := qt.New(t)
			want := mustParseSource(c, "schema.go", identityTrimFixture(""))

			got := mustParseSource(c, "schema.go", identityTrimFixture(around))

			c.Assert(got.Schemas, qt.DeepEquals, want.Schemas)
			c.Assert(got.Tables, qt.DeepEquals, want.Tables)
			c.Assert(got.Fields, qt.DeepEquals, want.Fields)
			c.Assert(got.Indexes, qt.DeepEquals, want.Indexes)
			c.Assert(got.Sequences, qt.DeepEquals, want.Sequences)
			c.Assert(got.Enums, qt.DeepEquals, want.Enums)
			c.Assert(got.Views, qt.DeepEquals, want.Views)
			c.Assert(got.ManagedData, qt.DeepEquals, want.ManagedData)
			c.Assert(got.Grants, qt.DeepEquals, want.Grants)
		})
	}
}

// The controls: the unspaced fixture holds the names as written, so the test
// above cannot pass on a parse that drops them, and a value that names no
// object keeps its space.
func TestParseSource_KeepsTheNamesOfTheTrimFixture(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", identityTrimFixture(" "))

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "counter")
	c.Assert(db.Sequences[0].Schema, qt.Equals, "app")
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Table, qt.Equals, "orders")
	c.Assert(db.ManagedData[0].Schema, qt.Equals, "app")
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "app")
	c.Assert(db.Fields, qt.HasLen, 3)
	c.Assert(db.Fields[1].Name, qt.Equals, "email")
	c.Assert(db.Fields[1].Default, qt.Equals, "'a '")
}
