package pgsnapshot_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/pgdefaultacl"
	"ptah.run/internal/pgsnapshot"
)

// startingPoint is a Supabase-shaped starting point: the auth schema, its
// users table with one column, and a function.
func startingPoint() pgsnapshot.Snapshot {
	return pgsnapshot.Snapshot{
		Objects: []pgsnapshot.Object{
			{Catalog: "pg_namespace", OID: 10, Kind: "schema", Name: `auth`, Drop: "DROP SCHEMA auth", Order: 90},
			{Catalog: "pg_class", OID: 20, Kind: "table", Name: "auth.users", Drop: "DROP TABLE auth.users", Order: 50},
			{Catalog: "pg_proc", OID: 21, Kind: "function", Name: "auth.uid()", Drop: "DROP FUNCTION auth.uid()", Order: 70},
		},
		Columns: []pgsnapshot.Column{
			{Relation: 20, Number: 1, Name: "auth.users.id", Drop: "ALTER TABLE auth.users DROP COLUMN id"},
		},
		Schemas: []string{"auth"},
	}
}

// TestPlanDropsWhatTheRunAddedInOrder pins the order a reset drops in: what
// hangs off a table first, then the columns the run added to a starting-point
// table, then indexes, relations and schemas. A column of a table the run
// created goes with its table, and the starting point is left alone.
func TestPlanDropsWhatTheRunAddedInOrder(t *testing.T) {
	c := qt.New(t)
	start := startingPoint()
	current := startingPoint()
	current.Objects = append(current.Objects,
		pgsnapshot.Object{Catalog: "pg_namespace", OID: 34, Kind: "schema", Name: "app", Drop: "DROP SCHEMA app", Order: 90},
		pgsnapshot.Object{Catalog: "pg_class", OID: 33, Kind: "table", Name: "auth.sessions", Drop: "DROP TABLE auth.sessions", Order: 50},
		pgsnapshot.Object{Catalog: "pg_class", OID: 32, Kind: "index", Name: "auth.users_email", Drop: "DROP INDEX auth.users_email", Order: 30},
		pgsnapshot.Object{Catalog: "pg_constraint", OID: 31, Kind: "constraint", Name: "email_set on auth.users", Drop: "DROP CONSTRAINT email_set", Order: 20},
		pgsnapshot.Object{Catalog: "pg_trigger", OID: 30, Kind: "trigger", Name: "on_signup on auth.users", Drop: "DROP TRIGGER on_signup", Order: 10},
	)
	current.Columns = append(current.Columns,
		pgsnapshot.Column{Relation: 20, Number: 2, Name: "auth.users.nickname", Drop: "ALTER TABLE auth.users DROP COLUMN nickname"},
		pgsnapshot.Column{Relation: 33, Number: 1, Name: "auth.sessions.id", Drop: "ALTER TABLE auth.sessions DROP COLUMN id"},
	)

	got := pgsnapshot.Plan(start, current)

	c.Assert(got, qt.DeepEquals, []string{
		"DROP TRIGGER on_signup",
		"DROP CONSTRAINT email_set",
		"ALTER TABLE auth.users DROP COLUMN nickname",
		"DROP INDEX auth.users_email",
		"DROP TABLE auth.sessions",
		"DROP SCHEMA app",
	})
}

// TestPlanDropsAnAddedColumnWhenTheRunAddedNoObject pins that an added column
// is dropped even when no object comes after it in the order.
func TestPlanDropsAnAddedColumnWhenTheRunAddedNoObject(t *testing.T) {
	c := qt.New(t)
	current := startingPoint()
	current.Columns = append(current.Columns,
		pgsnapshot.Column{Relation: 20, Number: 2, Name: "auth.users.nickname", Drop: "ALTER TABLE auth.users DROP COLUMN nickname"})

	got := pgsnapshot.Plan(startingPoint(), current)

	c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE auth.users DROP COLUMN nickname"})
}

// TestPlanReturnsEachChangedGranteeToItsPrivileges pins the privilege half:
// a grantee whose privileges differ is revoked and granted what the starting
// point gave it, with the grant option where it held one, and a grantee only
// the run added loses what it holds. A grantee whose privileges are the same
// is not touched, whoever granted them, since a statement the reset runs
// records its own role as the grantor. A column's list is spelled with the
// column, and the empty grantee is PUBLIC.
func TestPlanReturnsEachChangedGranteeToItsPrivileges(t *testing.T) {
	c := qt.New(t)
	start := startingPoint()
	start.Privileges = []pgsnapshot.Privileges{
		{Catalog: "pg_class", OID: 20, Target: "TABLE auth.users", ACL: []aclitem.Item{
			{Grantee: "postgres", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "SELECT"}, {Name: "INSERT"}}},
			{Grantee: "reader", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "SELECT"}}},
			{Grantee: "admin", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "SELECT", Grantable: true}}},
		}},
		{Catalog: "pg_class", OID: 20, Column: 1, Target: "TABLE auth.users", ColumnName: `"id"`, ACL: []aclitem.Item{
			{Grantee: "", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "SELECT"}}},
		}},
	}
	current := startingPoint()
	current.Privileges = []pgsnapshot.Privileges{
		{Catalog: "pg_class", OID: 20, Target: "TABLE auth.users", ACL: []aclitem.Item{
			{Grantee: "postgres", Grantor: "supabase_admin", Privileges: []aclitem.Privilege{{Name: "INSERT"}, {Name: "SELECT"}}},
			{Grantee: "reader", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "INSERT"}}},
			{Grantee: "writer", Grantor: "postgres", Privileges: []aclitem.Privilege{{Name: "UPDATE"}}},
		}},
		{Catalog: "pg_class", OID: 20, Column: 1, Target: "TABLE auth.users", ColumnName: `"id"`},
	}

	got := pgsnapshot.Plan(start, current)

	c.Assert(got, qt.DeepEquals, []string{
		`GRANT SELECT ON TABLE auth.users TO "admin" WITH GRANT OPTION`,
		`REVOKE ALL PRIVILEGES ON TABLE auth.users FROM "reader"`,
		`GRANT SELECT ON TABLE auth.users TO "reader"`,
		`REVOKE ALL PRIVILEGES ON TABLE auth.users FROM "writer"`,
		`GRANT SELECT ("id") ON TABLE auth.users TO PUBLIC`,
	})
}

// TestPlanReturnsDefaultPrivilegesInTheStartingPointSchemas pins that a
// default the run changed in a starting-point schema is set back, and one set
// in a schema the run created is left to go with the schema.
func TestPlanReturnsDefaultPrivilegesInTheStartingPointSchemas(t *testing.T) {
	c := qt.New(t)
	start := startingPoint()
	start.DefaultPrivileges = []pgdefaultacl.Row{{Schema: "auth", Grantor: "postgres", ObjectType: "TABLES",
		ACL: []aclitem.Item{{Grantee: "reader", Privileges: []aclitem.Privilege{{Name: "SELECT"}}}}}}
	current := startingPoint()
	current.DefaultPrivileges = []pgdefaultacl.Row{
		{Schema: "app", Grantor: "postgres", ObjectType: "TABLES",
			ACL: []aclitem.Item{{Grantee: "reader", Privileges: []aclitem.Privilege{{Name: "SELECT"}}}}},
		{Schema: "auth", Grantor: "postgres", ObjectType: "TABLES",
			ACL: []aclitem.Item{{Grantee: "reader", Privileges: []aclitem.Privilege{{Name: "DELETE"}}}}},
	}

	got := pgsnapshot.Plan(start, current)

	c.Assert(got, qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "auth" REVOKE ALL PRIVILEGES ON TABLES FROM "reader"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "auth" GRANT SELECT ON TABLES TO "reader"`,
	})
}

// TestPlanLeavesAnUnchangedDatabase pins that a database at its starting
// point plans nothing, which is what the reset's check after it relies on.
func TestPlanLeavesAnUnchangedDatabase(t *testing.T) {
	c := qt.New(t)

	got := pgsnapshot.Plan(startingPoint(), startingPoint())

	c.Assert(got, qt.HasLen, 0)
}

// TestMissingNamesWhatTheRunDroppedFromTheStartingPoint pins that an object
// and a column the starting point holds and a later read lacks are named, so
// the reset can refuse rather than commit a different starting point.
func TestMissingNamesWhatTheRunDroppedFromTheStartingPoint(t *testing.T) {
	c := qt.New(t)
	start := startingPoint()
	current := startingPoint()
	current.Objects = current.Objects[:2]
	current.Columns = nil

	got := pgsnapshot.Missing(start, current)

	c.Assert(got, qt.DeepEquals, []string{"column auth.users.id", "function auth.uid()"})
}

// TestMissingIgnoresWhatTheRunAdded pins the other direction: an added object
// is Plan's to drop, not a gap in the starting point.
func TestMissingIgnoresWhatTheRunAdded(t *testing.T) {
	c := qt.New(t)
	current := startingPoint()
	current.Objects = append(current.Objects,
		pgsnapshot.Object{Catalog: "pg_class", OID: 33, Kind: "table", Name: "auth.sessions"})

	got := pgsnapshot.Missing(startingPoint(), current)

	c.Assert(got, qt.HasLen, 0)
}
