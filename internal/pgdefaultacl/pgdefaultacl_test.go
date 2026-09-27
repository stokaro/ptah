package pgdefaultacl_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/pgdefaultacl"
)

// answerDefaultACL plays the server for [pgdefaultacl.ReadRevokes]: it records
// the statement and its arguments and answers rows, one pg_default_acl row
// each, as schema, grantor, class code, class keyword and array_to_json ACL.
func answerDefaultACL(rows [][]driver.Value, sent *[]string, bound *[][]driver.NamedValue) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		*sent = append(*sent, query)
		*bound = append(*bound, args)
		return dbtest.QueryResult{
			Columns: []string{"nspname", "grantor", "defaclobjtype", "object_type", "acl"},
			Rows:    rows,
		}, nil
	}
}

// TestReadRevokes_HappyPath builds one revoke per grantee from the ACL shapes
// the declared lines store, and orders them by schema, name and statement.
//
// The row with an empty grantor is CockroachDB's FOR ALL ROLES, role 0. The
// rows with nothing after the slash are CockroachDB's, which records no
// grantor in the ACL. The empty grantee is PUBLIC and is written as the
// keyword; a role named "PUBLIC" stays quoted, or its revoke would take the
// default from PUBLIC and leave the role's.
func TestReadRevokes_HappyPath(t *testing.T) {
	c := qt.New(t)
	var sent []string
	var bound [][]driver.NamedValue
	db := dbtest.Open(c, answerDefaultACL([][]driver.Value{
		{"app", "app_owner", "r", "TABLES", `["=r/app_owner","\"PUBLIC\"=r/app_owner","\"odd role\"=w*/app_owner"]`},
		{"app", "", "S", "SEQUENCES", `["app_reader=U*/"]`},
		{"app", "app_owner", "S", "SEQUENCES", `["app_reader=rU/"]`},
	}, &sent, &bound))

	revokes, err := pgdefaultacl.ReadRevokes(c.Context(), db.SQL, []string{"app"})

	c.Assert(err, qt.IsNil)
	c.Assert(revokes, qt.DeepEquals, []pgdefaultacl.Revoke{
		{
			Schema: "app", Class: "S", Grantee: "app_reader",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "app" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "app_reader"`,
		},
		{
			Schema: "app", Grantor: "app_owner", Class: "S", Grantee: "app_reader",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "app_reader"`,
		},
		{
			Schema: "app", Grantor: "app_owner", Class: "r", Grantee: "PUBLIC",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE ALL PRIVILEGES ON TABLES FROM "PUBLIC"`,
		},
		{
			Schema: "app", Grantor: "app_owner", Class: "r",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC`,
		},
		{
			Schema: "app", Grantor: "app_owner", Class: "r", Grantee: "odd role",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE ALL PRIVILEGES ON TABLES FROM "odd role"`,
		},
	})
	c.Assert(bound, qt.DeepEquals, [][]driver.NamedValue{{{Ordinal: 1, Value: "app"}}})
}

// TestRevoke_NamesHappyPath pins the labels a plan and a message show: role 0
// is `all roles`, the empty grantee is PUBLIC, and a named role is its name.
func TestRevoke_NamesHappyPath(t *testing.T) {
	tests := []struct {
		name        string
		revoke      pgdefaultacl.Revoke
		wantName    string
		wantGrantor string
		wantGrantee string
	}{
		{
			name:        "a role granting to a role",
			revoke:      pgdefaultacl.Revoke{Grantor: "app_owner", Class: "r", Grantee: "app_reader"},
			wantName:    "app_owner/r/app_reader",
			wantGrantor: "app_owner",
			wantGrantee: "app_reader",
		},
		{
			name:        "FOR ALL ROLES granting to PUBLIC",
			revoke:      pgdefaultacl.Revoke{Class: "T"},
			wantName:    "all roles/T/PUBLIC",
			wantGrantor: "all roles",
			wantGrantee: "PUBLIC",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.revoke.Name(), qt.Equals, test.wantName)
			c.Assert(test.revoke.GrantorName(), qt.Equals, test.wantGrantor)
			c.Assert(test.revoke.GranteeName(), qt.Equals, test.wantGrantee)
		})
	}
}

// TestReadRevokes_StatementReadsWhatEveryLineAnswers holds the statement to
// the parts whose absence reads as a plausible answer: the ACL selected whole
// rather than through aclexplode, which answers nothing on CockroachDB v25.4;
// the classes IN SCHEMA can name; and a match on every schema asked for.
func TestReadRevokes_StatementReadsWhatEveryLineAnswers(t *testing.T) {
	tests := []struct {
		name      string
		schemas   []string
		wantMatch string
		wantBound []driver.NamedValue
	}{
		{
			name:      "one schema",
			schemas:   []string{"app"},
			wantMatch: "WHERE n.nspname = $1 AND",
			wantBound: []driver.NamedValue{{Ordinal: 1, Value: "app"}},
		},
		{
			name:      "several schemas",
			schemas:   []string{"app", "public"},
			wantMatch: "WHERE n.nspname IN ($1, $2) AND",
			wantBound: []driver.NamedValue{{Ordinal: 1, Value: "app"}, {Ordinal: 2, Value: "public"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			var bound [][]driver.NamedValue
			db := dbtest.Open(c, answerDefaultACL(nil, &sent, &bound))

			revokes, err := pgdefaultacl.ReadRevokes(c.Context(), db.SQL, test.schemas)

			c.Assert(err, qt.IsNil)
			c.Assert(revokes, qt.HasLen, 0)
			c.Assert(sent, qt.HasLen, 1)
			query := strings.Join(strings.Fields(sent[0]), " ")
			c.Assert(query, qt.Contains, "COALESCE(array_to_json(d.defaclacl)::text, '[]') AS acl")
			c.Assert(query, qt.Not(qt.Contains), "aclexplode")
			c.Assert(query, qt.Contains, "d.defaclobjtype IN ('r', 'S', 'f', 'T')")
			c.Assert(query, qt.Contains, "WHEN d.defaclrole = 0 THEN ''")
			c.Assert(query, qt.Contains, test.wantMatch)
			c.Assert(bound, qt.DeepEquals, [][]driver.NamedValue{test.wantBound})
		})
	}
}

// TestReadRevokes_NoSchemaAsksNothing reads nothing for an empty scope: an IN
// list with nothing in it is a syntax error, and no schema holds nothing to
// revoke.
func TestReadRevokes_NoSchemaAsksNothing(t *testing.T) {
	c := qt.New(t)
	var sent []string
	var bound [][]driver.NamedValue
	db := dbtest.Open(c, answerDefaultACL(nil, &sent, &bound))

	revokes, err := pgdefaultacl.ReadRevokes(c.Context(), db.SQL, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(revokes, qt.IsNil)
	c.Assert(sent, qt.HasLen, 0)
}

// TestReadRevokes_FailurePath refuses an ACL it cannot read, naming the row,
// rather than leaving a grantee out: a skipped grantee is a default the
// cleanup leaves behind while reporting a clean schema. A refused query is
// returned as the server gave it.
func TestReadRevokes_FailurePath(t *testing.T) {
	t.Run("an ACL that does not parse", func(t *testing.T) {
		c := qt.New(t)
		var sent []string
		var bound [][]driver.NamedValue
		db := dbtest.Open(c, answerDefaultACL([][]driver.Value{
			{"app", "", "r", "TABLES", `["app_reader=rZ/"]`},
		}, &sent, &bound))

		revokes, err := pgdefaultacl.ReadRevokes(c.Context(), db.SQL, []string{"app"})

		c.Assert(err, qt.ErrorIs, aclitem.ErrMalformed)
		c.Assert(err, qt.ErrorMatches,
			`failed to read the default privileges all roles set on TABLES in schema app: malformed ACL item: .*`)
		c.Assert(revokes, qt.IsNil)
	})

	t.Run("a refused query", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("relation \"pg_default_acl\" does not exist")
		db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			return dbtest.QueryResult{}, refused
		})

		revokes, err := pgdefaultacl.ReadRevokes(c.Context(), db.SQL, []string{"app"})

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `failed to query default privileges: .*relation "pg_default_acl" does not exist`)
		c.Assert(revokes, qt.IsNil)
	})
}
