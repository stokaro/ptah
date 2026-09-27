package postgres

// White-box testing required: readDefaultPrivileges is unexported, and the
// exported ReadSchema path reaches it only through a live server. What is under
// test is partly the statement itself -- the inner join that drops the
// global entries and the object-type filter -- and a row the query never
// selects has no observation point in the rows it returns.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/aclitem"
	"ptah.run/internal/dbschema/dbtest"
)

// answerDefaultPrivileges plays PostgreSQL for the default-privilege read,
// answering the rows the bound schema owns and recording both the statements
// and the schemas they asked about. A row is one pg_default_acl row: grantor,
// schema, object type and the ACL as array_to_json renders it.
//
// The answer is keyed on the bound argument rather than returned unconditionally
// so a read that stopped binding the schema, or stopped looping the inspected
// schemas, reports the wrong rows rather than the right ones by accident.
func answerDefaultPrivileges(
	rowsBySchema map[string][][]driver.Value,
	sent, bound *[]string,
) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		*sent = append(*sent, query)
		schema := ""
		for _, arg := range args {
			name, _ := arg.Value.(string)
			schema = name
		}
		*bound = append(*bound, schema)
		return dbtest.QueryResult{
			Columns: []string{"grantor", "schema_name", "object_type", "acl"},
			Rows:    rowsBySchema[schema],
		}, nil
	}
}

// TestReadDefaultPrivileges_ProjectsOneRowPerPrivilegeHappyPath pins the grain
// and the per-privilege grantability.
//
// The two app_reader rows are one identity: the catalog stores a merged ACL
// per (defaclrole, defaclnamespace, defaclobjtype), and grantability is
// recorded per privilege inside it. A read that folded them into a single
// object with one flag would have to choose a grantability for the identity and
// then compare that choice against the catalog on every run.
//
// The schema comes back filled although this reader was not scoped and the row
// is in its own schema. Every other read here leaves the connected schema
// blank, but a default privilege's schema is its IN SCHEMA clause: blanked, it
// reaches the renderer as the global form, which the renderer refuses to write
// (stokaro/ptah#3732).
func TestReadDefaultPrivileges_ProjectsOneRowPerPrivilegeHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {
			{"app_owner", "public", "TABLES", `["app_reader=ra*/app_owner"]`},
			{"app_owner", "public", "SEQUENCES", `["=U/app_owner"]`},
		},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "app_owner", Schema: "public", ObjectType: "SEQUENCES", Grantee: "PUBLIC", Privilege: "USAGE"},
		{
			Grantor: "app_owner", Schema: "public", ObjectType: "TABLES", Grantee: "app_reader",
			Privilege: "INSERT", WithOption: true,
		},
		{Grantor: "app_owner", Schema: "public", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
	})
	c.Assert(bound, qt.DeepEquals, []string{"public"})
}

// TestReadDefaultPrivileges_ReadsEachLinesACLHappyPath reads the ACL each
// declared line stores, copied from what array_to_json answered on it.
//
// The CockroachDB v25.4.16 row is stokaro/ptah#3802: that line stores
// defaclacl as text[], writes a dashed name unquoted and leaves the grantor
// empty, and aclexplode over it answers no rows, so a read built on aclexplode
// describes no default privilege there. v26.3.1 quotes the same name, and
// PostgreSQL 18.6 names the grantor and quotes a name that holds a space.
func TestReadDefaultPrivileges_ReadsEachLinesACLHappyPath(t *testing.T) {
	tests := []struct {
		name string
		acl  string
	}{
		{name: "PostgreSQL 18.6", acl: `["\"odd-role.x\"=w*/ar_owner","ar_reader=a/ar_owner"]`},
		{name: "CockroachDB v25.4.16", acl: `["odd-role.x=w*/","ar_reader=a/"]`},
		{name: "CockroachDB v26.3.1", acl: `["\"odd-role.x\"=w*/","ar_reader=a/"]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent, bound []string
			db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
				"public": {{"ar_owner", "public", "TABLES", test.acl}},
			}, &sent, &bound))
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

			privileges, err := reader.readDefaultPrivileges(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
				{Grantor: "ar_owner", Schema: "public", ObjectType: "TABLES", Grantee: "ar_reader", Privilege: "INSERT"},
				{
					Grantor: "ar_owner", Schema: "public", ObjectType: "TABLES", Grantee: "odd-role.x",
					Privilege: "UPDATE", WithOption: true,
				},
			})
		})
	}
}

// TestReadDefaultPrivileges_ReadsEveryInspectedSchemaHappyPath is the control on
// the loop: one schema cannot tell a read that follows the inspected list from
// one that asks about the connected schema alone, and a description missing the
// second schema's defaults reads as "there are none" to the comparator.
//
// It also pins which schema a row carries: every row names its own, the
// connected one included, which is what a declaration names too, so both sides
// of a comparison spell the same object the same way.
func TestReadDefaultPrivileges_ReadsEveryInspectedSchemaHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {{"app_owner", "public", "TABLES", `["app_reader=r/app_owner"]`}},
		"app":    {{"app_owner", "app", "FUNCTIONS", `["app_reader=X/app_owner"]`}},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
	reader.SetSchemas([]string{"public", "app"})

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(bound, qt.DeepEquals, []string{"public", "app"})
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "app_owner", Schema: "public", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
		{
			Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS",
			Grantee: "app_reader", Privilege: "EXECUTE",
		},
	})
}

// TestReadDefaultPrivileges_LeavesReservedGranteesOutHappyPath holds the
// grantee to the reserved-name rule every role read applies: a pg_ role and the
// bootstrap superuser are left out, as an ordinary grant to them is. pgbouncer
// stays, because the reserved prefix is pg_ with its underscore, and PUBLIC is
// not a role name at all. The grantor is not filtered: the bootstrap
// superuser is the ordinary grantor of a default privilege.
func TestReadDefaultPrivileges_LeavesReservedGranteesOutHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {{
			"postgres", "public", "TABLES",
			`["pg_monitor=r/postgres","postgres=r/postgres","pgbouncer=r/postgres","=r/postgres"]`,
		}},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "postgres", Schema: "public", ObjectType: "TABLES", Grantee: "PUBLIC", Privilege: "SELECT"},
		{Grantor: "postgres", Schema: "public", ObjectType: "TABLES", Grantee: "pgbouncer", Privilege: "SELECT"},
	})
}

// TestReadDefaultPrivileges_OrdersRowsByteWiseHappyPath gives the rows and the
// ACL items in the reverse of the order a description lists them. The order is
// Ptah's, byte by byte, so an uppercase name sorts before a lowercase one
// whatever collation the server has, and two descriptions of one catalog cannot
// differ by row order.
func TestReadDefaultPrivileges_OrdersRowsByteWiseHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {
			{"b_owner", "public", "TABLES", `["r_reader=r/b_owner"]`},
			{"a_owner", "public", "TABLES", `["r_reader=r/a_owner","Upper=r/a_owner","=r/a_owner"]`},
		},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "a_owner", Schema: "public", ObjectType: "TABLES", Grantee: "PUBLIC", Privilege: "SELECT"},
		{Grantor: "a_owner", Schema: "public", ObjectType: "TABLES", Grantee: "Upper", Privilege: "SELECT"},
		{Grantor: "a_owner", Schema: "public", ObjectType: "TABLES", Grantee: "r_reader", Privilege: "SELECT"},
		{Grantor: "b_owner", Schema: "public", ObjectType: "TABLES", Grantee: "r_reader", Privilege: "SELECT"},
	})
}

// TestReadDefaultPrivileges_FailurePath refuses an ACL it cannot read rather
// than describing part of it: a privilege dropped because its letter was not
// understood is a default privilege the comparator would plan again on every
// run. The error names the row the ACL belongs to.
func TestReadDefaultPrivileges_FailurePath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {{"app_owner", "public", "TABLES", `["app_reader=rZ/app_owner"]`}},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.ErrorIs, aclitem.ErrMalformed)
	c.Assert(err, qt.ErrorMatches,
		`failed to read the default privileges app_owner set on TABLES in schema public: malformed ACL item: .*`)
	c.Assert(privileges, qt.IsNil)
}

// TestReadDefaultPrivileges_StatementKeepsTheRestrictionsNoRowCanShow asserts the
// parts of the query whose absence produces a plausible answer rather than a
// failure.
//
// Each one costs something different. An outer join to pg_namespace admits the
// global entries, which pg_default_acl records with defaclnamespace 0 and whose
// list holds the built-in default too, so exploding them here describes the
// owner's implicit rights as grants; the global read subtracts the built-in
// default instead. Without the complement of the undescribed predicate,
// CockroachDB's FOR ALL ROLES is described under the grantor `unknown
// (OID=0)`, a role nobody has. Dropping the object-type filter admits a class
// IN SCHEMA cannot name. And exploding the ACL with aclexplode reads nothing on
// CockroachDB v25.4.16, where the list is text[] (stokaro/ptah#3802).
func TestReadDefaultPrivileges_StatementKeepsTheRestrictionsNoRowCanShow(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(nil, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.HasLen, 0)
	c.Assert(sent, qt.HasLen, 1)
	query := strings.Join(strings.Fields(sent[0]), " ")
	c.Assert(query, qt.Contains, "JOIN pg_namespace n ON n.oid = d.defaclnamespace")
	c.Assert(query, qt.Not(qt.Contains), "LEFT JOIN pg_namespace")
	c.Assert(query, qt.Contains, "COALESCE(array_to_json(d.defaclacl)::text, '[]') AS acl")
	c.Assert(query, qt.Not(qt.Contains), "aclexplode")
	c.Assert(query, qt.Contains,
		"AND NOT (d.defaclrole = 0 OR (d.defaclnamespace = 0 AND d.defaclobjtype NOT IN ('r', 'S', 'f', 'T', 'n', 'L')))")
	c.Assert(query, qt.Contains, "d.defaclobjtype IN ('r', 'S', 'f', 'T')")
	c.Assert(query, qt.Not(qt.Contains), "format(",
		qt.Commentf("a read projects columns; the statement text belongs to the renderer"))
}
