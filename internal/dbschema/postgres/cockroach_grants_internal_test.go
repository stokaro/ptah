package postgres

// White-box testing required: readCockroachGrants is unexported, and the
// exported ReadSchemaContext reaches it only after two dozen other catalog
// statements a fake would have to answer consistently. What is under test is
// the statements' restrictions -- the built-in roles and the owners they leave
// out -- as much as the rows, and a row the query never selects has no
// observation point in the rows it returns.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// answerCockroachGrants plays CockroachDB for the three information_schema
// reads, with rows shaped as the reads select them, and records every
// statement.
func answerCockroachGrants(sent *[]string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*sent = append(*sent, query)
		switch {
		case strings.Contains(query, "information_schema.role_table_grants"):
			return dbtest.QueryResult{
				Columns: []string{"grantee", "privilege_type", "relname", "sequence", "with_option"},
				Rows: [][]driver.Value{
					{"g_reader", "INSERT", "t", false, true},
					{"g_reader", "SELECT", "t", false, false},
					{"PUBLIC", "SELECT", "v", false, false},
					{"g_reader", "USAGE", "s", true, false},
					{"g_reader", "USAGE", "t_id_seq", true, false},
				},
			}, nil
		case strings.Contains(query, "information_schema.schema_privileges"):
			return dbtest.QueryResult{
				Columns: []string{"grantee", "privilege_type", "with_option"},
				Rows:    [][]driver.Value{{"g_reader", "USAGE", false}},
			}, nil
		default:
			return dbtest.QueryResult{
				Columns: []string{"proname", "args", "kind", "grantee", "privilege_type", "with_option", "owner"},
				Rows: [][]driver.Value{
					{"f", "", "FUNCTION", "PUBLIC", "EXECUTE", false, false},
					{"f", "", "FUNCTION", "g_reader", "EXECUTE", false, false},
					{"f", "", "FUNCTION", "root", "ALL", true, true},
					{"untouched_f", "", "FUNCTION", "root", "ALL", true, true},
				},
			}, nil
		}
	}
}

// TestReadCockroachGrants_HappyPath reads what information_schema answered on
// CockroachDB v25.4.16, v26.2.7 and v26.3.1 alike for the fixture of
// stokaro/ptah#3815, where the ACL columns are NULL on the first two.
//
// A sequence is told from a table by its relation kind, and t_id_seq, which a
// column owns, is kept beside the standalone s. PUBLIC's EXECUTE on f is the
// default and reads as implicit. The owner of each routine reads as an
// implicit EXECUTE whoever it is, root here: untouched_f's owner row is what
// says the routine was read and PUBLIC holds nothing on it, which is the
// revoke a description has to carry.
func TestReadCockroachGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	var sent []string
	db := dbtest.Open(c, answerCockroachGrants(&sent))
	reader := NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", platform.CockroachDB, capability.CockroachDB26())
	reader.SetSchemas([]string{"app"})

	grants, err := reader.readCockroachGrants(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(grants, qt.DeepEquals, []catalog.Grant{
		{Role: "g_reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "t", Schema: "app", WithOption: true},
		{Role: "g_reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Schema: "app"},
		{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v", Schema: "app"},
		{Role: "g_reader", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "s", Schema: "app"},
		{Role: "g_reader", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "t_id_seq", Schema: "app"},
		{Role: "g_reader", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f", Schema: "app", Implicit: true},
		{Role: "g_reader", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f", Schema: "app"},
		{Role: "root", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f", Schema: "app", Implicit: true},
		{
			Role: "root", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "untouched_f", Schema: "app",
			Implicit: true,
		},
	})
}

// TestReadCockroachGrants_StatementsLeaveOutWhatNobodyGranted pins the
// restrictions whose absence reads as extra grants rather than as a failure:
// admin and root hold ALL on every object, and an owner holds ALL on what it
// owns, and none of them can be revoked. A routine's owner is the exception
// the routine read keeps, as an implicit row. The ACL columns are not read.
func TestReadCockroachGrants_StatementsLeaveOutWhatNobodyGranted(t *testing.T) {
	c := qt.New(t)
	var sent []string
	db := dbtest.Open(c, answerCockroachGrants(&sent))
	reader := NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", platform.CockroachDB, capability.CockroachDB26())
	reader.SetSchemas([]string{"app"})

	_, err := reader.readCockroachGrants(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(sent, qt.HasLen, 3)
	relations := strings.Join(strings.Fields(sent[0]), " ")
	c.Assert(relations, qt.Contains, "AND g.grantee NOT IN ('admin', 'root')")
	c.Assert(relations, qt.Contains, "AND g.grantee <> pg_get_userbyid(c.relowner)")
	schemas := strings.Join(strings.Fields(sent[1]), " ")
	c.Assert(schemas, qt.Contains, "AND p.grantee NOT IN ('admin', 'root')")
	c.Assert(schemas, qt.Contains, "AND p.grantee <> pg_get_userbyid(n.nspowner)")
	routines := strings.Join(strings.Fields(sent[2]), " ")
	c.Assert(routines, qt.Contains,
		"AND (g.grantee NOT IN ('admin', 'root') OR g.grantee = pg_get_userbyid(p.proowner))")
	c.Assert(routines, qt.Contains, "JOIN pg_proc p ON p.proname || '_' || p.oid::text = g.specific_name")
	for _, statement := range sent {
		c.Assert(statement, qt.Not(qt.Contains), "aclexplode")
	}
}
