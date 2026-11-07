package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
)

// heldGrants are the grants of a database that holds app.items: one on items
// and one on a column of it, one on each of another table, a table of the
// same name in another schema and the schema app, and a partial revoke on
// items.
func heldGrants() []catalog.Grant {
	return []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app", ObjectName: "items"},
		{Role: "writer", Privilege: "UPDATE", ObjectType: "table", Schema: "app", ObjectName: "items", Column: "note"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app", ObjectName: "orders"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "items"},
		{Role: "reader", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
		{Role: "owner", Privilege: "DELETE", ObjectType: "TABLE", Schema: "app", ObjectName: "items", Implicit: true, IsPartialRevoke: true},
	}
}

// A table's observation holds the grants the read found on the table itself,
// a column privilege among them, under the table's identity: a grant on
// another table, on a table of the name in another schema, on the schema, and
// a partial revoke, which grants nothing, are left out.
func TestTableObservationCapturesTheTablesGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{Grants: heldGrants()}

	observation := difftypes.TableObservationFor(current, catalog.Table{Name: "items", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

	c.Assert(observation.GrantsDescribed, qt.IsTrue)
	c.Assert(observation.Grants, qt.DeepEquals, heldGrants()[:2])
	current.Grants[0].Role = "changed"
	c.Assert(observation.Grants[0].Role, qt.Equals, "reader")
	c.Assert(observation.Clone().Grants, qt.DeepEquals, heldGrants()[:2])
}

// A read that did not describe grants leaves the observation's grants
// undescribed, so an empty list cannot read as a table without grants; an
// observation with no read describes none either.
func TestTableObservationKnowsWhenGrantsWereNotRead_HappyPath(t *testing.T) {
	t.Run("a read that did not describe grants", func(t *testing.T) {
		c := qt.New(t)
		current := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.Grant))}

		observation := difftypes.TableObservationFor(current, catalog.Table{Name: "items", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

		c.Assert(observation.GrantsDescribed, qt.IsFalse)
		c.Assert(observation.Grants, qt.IsNil)
	})
	t.Run("no read", func(t *testing.T) {
		c := qt.New(t)

		observation := difftypes.TableObservationFor(nil, catalog.Table{Name: "items", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

		c.Assert(observation.GrantsDescribed, qt.IsFalse)
		c.Assert(observation.Grants, qt.IsNil)
	})
}
