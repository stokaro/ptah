package pgdefaultacl_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/pgdefaultacl"
)

// parseACL reads an ACL as array_to_json renders it, for a fixture.
func parseACL(c *qt.C, encoded string) []aclitem.Item {
	c.Helper()
	items, err := aclitem.ParseJSON(encoded)
	c.Assert(err, qt.IsNil)
	return items
}

// TestGlobalDeltas_SubtractsTheBuiltInDefault reads the global rows PostgreSQL
// 18.6 stored for the statements in each row's name, against acldefault for
// the same grantor and class. A global row holds the built-in default too, so
// what it declares is its difference from it (stokaro/ptah#3772).
func TestGlobalDeltas_SubtractsTheBuiltInDefault(t *testing.T) {
	tests := []struct {
		name    string
		row     string
		builtin string
		want    []pgdefaultacl.Delta
	}{
		{
			name:    "GRANT SELECT ON TABLES TO r",
			row:     `["o=arwdDxtm/o","r=r/o"]`,
			builtin: `["o=arwdDxtm/o"]`,
			want:    []pgdefaultacl.Delta{{Grantee: "r", Privilege: "SELECT"}},
		},
		{
			name:    "REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
			row:     `["o=X/o"]`,
			builtin: `["=X/o","o=X/o"]`,
			want:    []pgdefaultacl.Delta{{Grantee: "", Privilege: "EXECUTE", Revoked: true}},
		},
		{
			name:    "GRANT USAGE ON SEQUENCES TO r WITH GRANT OPTION",
			row:     `["o=rwU/o","r=U*/o"]`,
			builtin: `["o=rwU/o"]`,
			want:    []pgdefaultacl.Delta{{Grantee: "r", Privilege: "USAGE", WithOption: true}},
		},
		{
			name:    "REVOKE DELETE ON TABLES FROM the owner",
			row:     `["o=arwDxtm/o"]`,
			builtin: `["o=arwdDxtm/o"]`,
			want:    []pgdefaultacl.Delta{{Grantee: "o", Privilege: "DELETE", Revoked: true}},
		},
		{
			// A built-in privilege held with the grant option is a grant of
			// the option, not a second privilege.
			name:    "GRANT EXECUTE ON FUNCTIONS TO PUBLIC WITH GRANT OPTION",
			row:     `["=X*/o","o=X/o"]`,
			builtin: `["=X/o","o=X/o"]`,
			want:    []pgdefaultacl.Delta{{Grantee: "", Privilege: "EXECUTE", WithOption: true}},
		},
		{
			name:    "a grant and a revoke of one grantee, in order",
			row:     `["o=X/o","r2=X/o"]`,
			builtin: `["=X/o","o=X/o"]`,
			want: []pgdefaultacl.Delta{
				{Grantee: "", Privilege: "EXECUTE", Revoked: true},
				{Grantee: "r2", Privilege: "EXECUTE"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgdefaultacl.GlobalDeltas(parseACL(c, test.row), parseACL(c, test.builtin))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestBuiltin_NamesTheBuiltInDefault pins the rule acldefault applies on
// PostgreSQL 18.6: the owner holds every privilege of the class, and ALL;
// PUBLIC holds EXECUTE on functions and USAGE on types; nobody else holds
// anything.
func TestBuiltin_NamesTheBuiltInDefault(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		grantee    string
		privilege  string
		want       bool
	}{
		{name: "the owner's SELECT on tables", objectType: "TABLES", grantee: "o", privilege: "SELECT", want: true},
		{name: "the owner's ALL on schemas", objectType: "SCHEMAS", grantee: "o", privilege: "ALL", want: true},
		{name: "the owner's UPDATE on large objects", objectType: "LARGE OBJECTS", grantee: "o", privilege: "UPDATE", want: true},
		{name: "the owner's EXECUTE on tables", objectType: "TABLES", grantee: "o", privilege: "EXECUTE", want: false},
		{name: "PUBLIC's EXECUTE on functions", objectType: "FUNCTIONS", grantee: "PUBLIC", privilege: "EXECUTE", want: true},
		{name: "PUBLIC's USAGE on types, any case", objectType: "types", grantee: "public", privilege: "usage", want: true},
		{name: "PUBLIC's SELECT on tables", objectType: "TABLES", grantee: "PUBLIC", privilege: "SELECT", want: false},
		{name: "another role's EXECUTE on functions", objectType: "FUNCTIONS", grantee: "r", privilege: "EXECUTE", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgdefaultacl.Builtin(test.objectType, "o", test.grantee, test.privilege), qt.Equals, test.want)
		})
	}
}

// TestReadGlobalResets_ReturnsEachRowToTheBuiltInDefault spells what a realm
// cleanup runs: a revoke of everything for each grantee a row differs on, then
// a grant of what the built-in default holds for the owner and for PUBLIC. The
// revoke comes first, or it would take back what the grant restored. The
// statements carry no IN SCHEMA, because the rows have none. A role named
// "PUBLIC" is an ordinary role: it is revoked by its quoted name and gets
// nothing back.
func TestReadGlobalResets_ReturnsEachRowToTheBuiltInDefault(t *testing.T) {
	c := qt.New(t)
	var sent []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		sent = append(sent, query)
		return dbtest.QueryResult{
			Columns: []string{"grantor", "object_type", "acl", "builtin"},
			Rows: [][]driver.Value{
				{"o", "FUNCTIONS", `["o=X/o","\"PUBLIC\"=X/o"]`, `["=X/o","o=X/o"]`},
				{"o", "TABLES", `["o=arwDxtm/o","r=r/o"]`, `["o=arwdDxtm/o"]`},
			},
		}, nil
	})

	resets, err := pgdefaultacl.ReadGlobalResets(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(resets))
	for _, reset := range resets {
		statements = append(statements, reset.Name()+": "+reset.Statement)
	}
	c.Assert(statements, qt.DeepEquals, []string{
		`o/f/PUBLIC: ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC`,
		`o/f/PUBLIC: ALTER DEFAULT PRIVILEGES FOR ROLE "o" GRANT EXECUTE ON FUNCTIONS TO PUBLIC`,
		`o/f/PUBLIC: ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM "PUBLIC"`,
		`o/r/o: ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON TABLES FROM "o"`,
		`o/r/o: ALTER DEFAULT PRIVILEGES FOR ROLE "o" GRANT ALL PRIVILEGES ON TABLES TO "o"`,
		`o/r/r: ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON TABLES FROM "r"`,
	})
	c.Assert(sent, qt.HasLen, 1)
	query := strings.Join(strings.Fields(sent[0]), " ")
	c.Assert(query, qt.Contains, "acldefault(CASE d.defaclobjtype WHEN 'S' THEN 's' ELSE d.defaclobjtype END, d.defaclrole)")
	c.Assert(query, qt.Contains, "d.defaclnamespace = 0 AND NOT")
}

// answerGlobalShow plays CockroachDB for [pgdefaultacl.ReadGlobalFromShow]:
// the role list, and SHOW DEFAULT PRIVILEGES without IN SCHEMA, which names
// the built-in rows too.
func answerGlobalShow(roles []string, shown [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.HasPrefix(query, "SELECT rolname FROM pg_roles") {
			rows := make([][]driver.Value, 0, len(roles))
			for _, role := range roles {
				rows = append(rows, []driver.Value{role})
			}
			return dbtest.QueryResult{Columns: []string{"rolname"}, Rows: rows}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"role", "for_all_roles", "object_type", "grantee", "privilege_type", "is_grantable"},
			Rows:    shown,
		}, nil
	}
}

// builtinShow is what SHOW DEFAULT PRIVILEGES names for a role nobody changed,
// measured on CockroachDB v26.3.2: the owner's ALL, with the grant option, on
// every class, and PUBLIC's EXECUTE on routines and USAGE on types.
func builtinShow(role string) [][]driver.Value {
	return [][]driver.Value{
		{role, false, "routines", "public", "EXECUTE", false},
		{role, false, "routines", role, "ALL", true},
		{role, false, "schemas", role, "ALL", true},
		{role, false, "sequences", role, "ALL", true},
		{role, false, "tables", role, "ALL", true},
		{role, false, "types", "public", "USAGE", false},
		{role, false, "types", role, "ALL", true},
	}
}

// TestReadGlobalFromShow_SubtractsCockroachDBsBuiltInDefault reads the shapes
// SHOW names on CockroachDB v26.3.2. A role nobody changed differs on nothing;
// a grant to another role is a grant; PUBLIC's missing USAGE on types is a
// revoke; an owner holding nothing on tables revoked ALL; and an owner
// holding some of its privileges cannot be named, so it is reported rather
// than described.
func TestReadGlobalFromShow_SubtractsCockroachDBsBuiltInDefault(t *testing.T) {
	c := qt.New(t)
	shown := builtinShow("untouched")
	shown = append(shown,
		[]driver.Value{"o", false, "routines", "public", "EXECUTE", false},
		[]driver.Value{"o", false, "routines", "o", "ALL", true},
		[]driver.Value{"o", false, "schemas", "o", "ALL", true},
		[]driver.Value{"o", false, "sequences", "o", "ALL", true},
		[]driver.Value{"o", false, "sequences", "r", "USAGE", true},
		[]driver.Value{"o", false, "tables", "r", "SELECT", false},
		[]driver.Value{"o", false, "types", "o", "ALL", true},
		[]driver.Value{"y", false, "routines", "public", "EXECUTE", false},
		[]driver.Value{"y", false, "routines", "y", "ALL", true},
		[]driver.Value{"y", false, "schemas", "y", "ALL", true},
		[]driver.Value{"y", false, "sequences", "y", "ALL", true},
		[]driver.Value{"y", false, "tables", "y", "DELETE", false},
		[]driver.Value{"y", false, "types", "public", "USAGE", false},
		[]driver.Value{"y", false, "types", "y", "ALL", true},
		[]driver.Value{nil, true, "tables", "r", "SELECT", false},
	)
	db := dbtest.Open(c, answerGlobalShow([]string{"o", "untouched", "y"}, shown))

	classes, err := pgdefaultacl.ReadGlobalFromShow(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(classes, qt.DeepEquals, []pgdefaultacl.GlobalClass{
		{Grantor: "o", ObjectType: "SEQUENCES", Deltas: []pgdefaultacl.Delta{
			{Grantee: "r", Privilege: "USAGE", WithOption: true},
		}},
		{Grantor: "o", ObjectType: "TABLES", Deltas: []pgdefaultacl.Delta{
			{Grantee: "o", Privilege: "ALL", Revoked: true},
			{Grantee: "r", Privilege: "SELECT"},
		}},
		{Grantor: "o", ObjectType: "TYPES", Deltas: []pgdefaultacl.Delta{
			{Grantee: "", Privilege: "USAGE", Revoked: true},
		}},
		{Grantor: "y", ObjectType: "TABLES", OwnerUndescribed: true},
	})
}

// TestReadGlobalResetsFromShow_ResetsTheOwnerItCannotName resets an owner
// whose own privileges SHOW could not name: granting ALL back returns it to
// the built-in default whatever it lost.
func TestReadGlobalResetsFromShow_ResetsTheOwnerItCannotName(t *testing.T) {
	c := qt.New(t)
	shown := [][]driver.Value{
		{"y", false, "routines", "public", "EXECUTE", false},
		{"y", false, "routines", "y", "ALL", true},
		{"y", false, "schemas", "y", "ALL", true},
		{"y", false, "sequences", "y", "ALL", true},
		{"y", false, "tables", "y", "DELETE", false},
		{"y", false, "types", "public", "USAGE", false},
		{"y", false, "types", "y", "ALL", true},
	}
	db := dbtest.Open(c, answerGlobalShow([]string{"y"}, shown))

	resets, err := pgdefaultacl.ReadGlobalResetsFromShow(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(resets))
	for _, reset := range resets {
		statements = append(statements, reset.Statement)
	}
	c.Assert(statements, qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "y" REVOKE ALL PRIVILEGES ON TABLES FROM "y"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "y" GRANT ALL PRIVILEGES ON TABLES TO "y"`,
	})
}
