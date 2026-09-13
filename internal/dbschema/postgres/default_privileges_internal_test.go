package postgres

// White-box testing required: readDefaultPrivileges is unexported, and the
// exported ReadSchema path reaches it only through a live server. What is under
// test is partly the statement itself -- the inner join that drops the
// cluster-wide entries, the object-type filter and the escaped reserved-name
// exclusion -- and a row the query never selects has no observation point in
// the rows it returns.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// answerDefaultPrivileges plays PostgreSQL for the default-privilege read,
// answering the rows the bound schema owns and recording both the statements
// and the schemas they asked about.
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
			Columns: []string{
				"grantor", "schema_name", "object_type", "grantee", "privilege", "with_option",
			},
			Rows: rowsBySchema[schema],
		}, nil
	}
}

// TestReadDefaultPrivileges_ProjectsOneRowPerPrivilegeHappyPath pins the grain
// and the per-privilege grantability.
//
// The two app_reader rows are one identity: the catalog stores a merged
// aclitem[] per (defaclrole, defaclnamespace, defaclobjtype), and the server
// explodes it into one row per privilege with its own is_grantable. A read that
// folded them into a single object with one flag would have to choose a
// grantability for the identity and then compare that choice against the
// catalog on every run.
//
// The schema comes back empty because this reader was not scoped and the row is
// in its own schema: that is the same convention every other read here follows,
// so an object in the connected schema stays unqualified.
func TestReadDefaultPrivileges_ProjectsOneRowPerPrivilegeHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {
			{"app_owner", "public", "TABLES", "app_reader", "SELECT", false},
			{"app_owner", "public", "TABLES", "app_reader", "INSERT", true},
			{"app_owner", "public", "SEQUENCES", "PUBLIC", "USAGE", false},
		},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
		{
			Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader",
			Privilege: "INSERT", WithOption: true,
		},
		{Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "PUBLIC", Privilege: "USAGE"},
	})
	c.Assert(bound, qt.DeepEquals, []string{"public"})
}

// TestReadDefaultPrivileges_ReadsEveryInspectedSchemaHappyPath is the control on
// the loop: one schema cannot tell a read that follows the inspected list from
// one that asks about the connected schema alone, and a description missing the
// second schema's defaults reads as "there are none" to the comparator.
//
// It also pins which schema a row carries: the connected one stays unqualified
// and the others are named, so both sides of a comparison spell the same object
// the same way.
func TestReadDefaultPrivileges_ReadsEveryInspectedSchemaHappyPath(t *testing.T) {
	c := qt.New(t)
	var sent, bound []string
	db := dbtest.Open(c, answerDefaultPrivileges(map[string][][]driver.Value{
		"public": {{"app_owner", "public", "TABLES", "app_reader", "SELECT", false}},
		"app":    {{"app_owner", "app", "FUNCTIONS", "app_reader", "EXECUTE", false}},
	}, &sent, &bound))
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
	reader.SetSchemas([]string{"public", "app"})

	privileges, err := reader.readDefaultPrivileges(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(bound, qt.DeepEquals, []string{"public", "app"})
	c.Assert(privileges, qt.DeepEquals, []catalog.DefaultPrivilege{
		{Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
		{
			Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS",
			Grantee: "app_reader", Privilege: "EXECUTE",
		},
	})
}

// TestReadDefaultPrivileges_StatementKeepsTheRestrictionsNoRowCanShow asserts the
// parts of the query whose absence produces a plausible answer rather than a
// failure.
//
// Each one costs something different. An outer join to pg_namespace admits the
// cluster-wide entries, which pg_default_acl records with defaclnamespace 0 and
// which replay refuses because they carry no IN SCHEMA. Dropping the object-type
// filter admits defaclobjtype 'n', for which the CASE yields NULL and no
// statement exists. An unescaped underscore in the reserved-name exclusion reads
// as a single-character wildcard and takes pgbouncer, pgadmin and pgpool with
// it. And an unordered read makes two descriptions of one server differ by row
// order alone.
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
	c.Assert(query, qt.Contains, "CROSS JOIN LATERAL aclexplode(d.defaclacl) acl")
	c.Assert(query, qt.Contains, "d.defaclobjtype IN ('r', 'S', 'f', 'T')")
	c.Assert(query, qt.Contains, `grantee NOT LIKE 'pg\_%' ESCAPE '\'`)
	c.Assert(query, qt.Not(qt.Contains), `NOT LIKE 'pg_%'`)
	c.Assert(query, qt.Contains, "ORDER BY schema_name, grantor, object_type, grantee, privilege")
	c.Assert(query, qt.Not(qt.Contains), "format(",
		qt.Commentf("a read projects columns; the statement text belongs to the renderer"))
}
