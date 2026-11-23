package yamlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// identityTrimDocument declares one entry of every kind whose schema, name or
// table a plan keys on. Each %s is where a space goes around a value.
const identityTrimDocument = `
enums:
  "%sstatus%s": [on, off]
tables:
  orders:
    name: "%sorders%s"
    schema: "%sapp%s"
    columns:
      id:
        name: "%sid%s"
        type: INTEGER
        primary: true
      email:
        name: "%semail%s"
        type: TEXT
        default: "'a '"
    indexes:
      by_email:
        name: "%sorders_email%s"
        fields: [email]
    constraints:
      email_set:
        name: "%semail_set%s"
        type: CHECK
        check: "email <> ''"
indexes:
  top_email:
    name: "%sorders_email_top%s"
    table: "%sorders%s"
    fields: [email]
views:
  v_orders:
    name: "%sv_orders%s"
    body: SELECT 1
triggers:
  orders_touch:
    name: "%sorders_touch%s"
    table: "%sorders%s"
    timing: BEFORE
    event: UPDATE
    body: RETURN NEW;
rls_policies:
  orders_owner:
    name: "%sorders_owner%s"
    table: "%sorders%s"
    using: "true"
rls_enabled_tables:
  orders:
    table: "%sorders%s"
roles:
  reader:
    name: "%sreader%s"
grants:
  reader_app:
    role: reader
    privileges: [USAGE]
    on_schema: "%sapp%s"
`

// identityTrimModel parses the document with around before and after every
// value it marks.
func identityTrimModel(c *qt.C, around string) *schemamodel.Database {
	c.Helper()
	db, err := yamlschema.Parse(c.Context(), yamlext.None(), []byte(strings.ReplaceAll(identityTrimDocument, "%s", around)))
	c.Assert(err, qt.IsNil)
	return db
}

// A document is text an author typed, so the space around a schema, a name or a
// table is removed where the document is read: it parses to the model the
// same document without the space does. Every layer below keeps a name's
// bytes, so a value read with its space names another object, and a table
// entry with schema "app " beside a live app.orders planned a DROP of
// app.orders.
func TestParse_TrimsTheSpaceAroundAnIdentityField(t *testing.T) {
	for _, around := range []string{" ", "\t", "  "} {
		t.Run(strings.ReplaceAll(around, "\t", `\t`), func(t *testing.T) {
			c := qt.New(t)
			want := identityTrimModel(c, "")

			got := identityTrimModel(c, around)

			c.Assert(got.Enums, qt.DeepEquals, want.Enums)
			c.Assert(got.Tables, qt.DeepEquals, want.Tables)
			c.Assert(got.Fields, qt.DeepEquals, want.Fields)
			c.Assert(got.Indexes, qt.DeepEquals, want.Indexes)
			c.Assert(got.Constraints, qt.DeepEquals, want.Constraints)
			c.Assert(got.Views, qt.DeepEquals, want.Views)
			c.Assert(got.Triggers, qt.DeepEquals, want.Triggers)
			c.Assert(got.RLSPolicies, qt.DeepEquals, want.RLSPolicies)
			c.Assert(got.RLSEnabledTables, qt.DeepEquals, want.RLSEnabledTables)
			c.Assert(got.Roles, qt.DeepEquals, want.Roles)
			c.Assert(got.Grants, qt.DeepEquals, want.Grants)
		})
	}
}

// The controls: the spaced document holds the names without the space, so the
// test above cannot pass on a parse that drops them, and a value that names no
// object keeps its space.
func TestParse_KeepsTheNamesOfTheTrimDocument(t *testing.T) {
	c := qt.New(t)

	db := identityTrimModel(c, " ")

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
	c.Assert(db.Tables[0].Schema, qt.Equals, "app")
	c.Assert(db.Enums, qt.HasLen, 1)
	c.Assert(db.Enums[0].Name, qt.Equals, "status")
	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Name, qt.Equals, "v_orders")
	c.Assert(db.Triggers, qt.HasLen, 1)
	// The table resolves to the declared app.orders, which a table name read
	// with its space would not.
	c.Assert(db.Triggers[0].Table, qt.Equals, "app.orders")
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "app")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[1].Name, qt.Equals, "email")
	c.Assert(db.Fields[1].Default, qt.Equals, "'a '")
}
