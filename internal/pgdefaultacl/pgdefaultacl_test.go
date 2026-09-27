package pgdefaultacl_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
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

// sqlStateError is a driver error carrying a SQLSTATE, as pgconn.PgError does.
type sqlStateError struct {
	state   string
	message string
}

func (e sqlStateError) Error() string    { return "ERROR: " + e.message + " (SQLSTATE " + e.state + ")" }
func (e sqlStateError) SQLState() string { return e.state }

// cockroachRefusal is the error CockroachDB v26.2.7 answered a read of
// pg_default_acl with, once a default named the role r-dash.
var cockroachRefusal = sqlStateError{state: "22P02", message: `missing "=" sign: "r-dash=U*/"`}

// TestRefused_RecognizesOnlyTheRefusal holds the predicate to the one error it
// names: the SQLSTATE and the message together. Either alone is a different
// failure, which the read must report rather than record as a refusal.
func TestRefused_RecognizesOnlyTheRefusal(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "the refusal", err: cockroachRefusal, want: true},
		{name: "the refusal, wrapped", err: fmt.Errorf("read: %w", cockroachRefusal), want: true},
		{
			name: "another invalid text representation",
			err:  sqlStateError{state: "22P02", message: `invalid input syntax for type integer: "x"`},
			want: false,
		},
		{
			name: "the message under another SQLSTATE",
			err:  sqlStateError{state: "XX000", message: `missing "=" sign: "r-dash=U*/"`},
			want: false,
		},
		{name: "an error with no SQLSTATE", err: errors.New(`missing "=" sign`), want: false},
		{name: "no error", err: nil, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgdefaultacl.Refused(test.err), qt.Equals, test.want)
		})
	}
}

// TestReadable_HappyPath answers false for the refusal whether it comes with
// the statement or after the rows before the refused one, which is how
// CockroachDB v26.2.7 delivers it to a full read, and true for a relation it
// read to the end.
func TestReadable_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		result dbtest.QueryResult
		err    error
		want   bool
	}{
		{
			name:   "every row arrives",
			result: dbtest.QueryResult{Columns: []string{"?column?"}, Rows: [][]driver.Value{{int64(1)}, {int64(1)}}},
			want:   true,
		},
		{
			name: "the refusal after a row",
			result: dbtest.QueryResult{
				Columns: []string{"?column?"}, Rows: [][]driver.Value{{int64(1)}}, TerminalErr: cockroachRefusal,
			},
			want: false,
		},
		{name: "the refusal with the statement", err: cockroachRefusal, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				sent = append(sent, query)
				return test.result, test.err
			})

			readable, err := pgdefaultacl.Readable(c.Context(), db.SQL)

			c.Assert(err, qt.IsNil)
			c.Assert(readable, qt.Equals, test.want)
			c.Assert(sent, qt.DeepEquals, []string{"SELECT 1 FROM pg_default_acl"})
		})
	}
}

// TestReadable_FailurePath returns any other failure, from the statement or
// from the rows, rather than taking it for the refusal.
func TestReadable_FailurePath(t *testing.T) {
	broken := sqlStateError{state: "08006", message: "connection failure"}
	tests := []struct {
		name   string
		result dbtest.QueryResult
		err    error
	}{
		{name: "with the statement", err: broken},
		{name: "after the rows", result: dbtest.QueryResult{Columns: []string{"?column?"}, TerminalErr: broken}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return test.result, test.err
			})

			readable, err := pgdefaultacl.Readable(c.Context(), db.SQL)

			c.Assert(err, qt.ErrorIs, broken)
			c.Assert(err, qt.ErrorMatches, `failed to read pg_default_acl: ERROR: connection failure \(SQLSTATE 08006\)`)
			c.Assert(readable, qt.IsFalse)
		})
	}
}

// answerShow plays CockroachDB v26.2.7 for [pgdefaultacl.ReadRevokesFromShow],
// with the rows that server printed: a FOR ALL ROLES row has no role, PUBLIC
// is `public`, functions are `routines`, and a grantee holding several
// privileges comes once per privilege.
func answerShow(sent *[]string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*sent = append(*sent, query)
		columns := []string{"role", "for_all_roles", "object_type", "grantee", "privilege_type", "is_grantable"}
		switch {
		case query == "SELECT rolname FROM pg_roles ORDER BY rolname":
			return dbtest.QueryResult{Columns: []string{"rolname"}, Rows: [][]driver.Value{{"d-dash"}, {"d_owner"}}}, nil
		case strings.HasPrefix(query, "SHOW DEFAULT PRIVILEGES FOR ALL ROLES"):
			return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{
				{nil, true, "sequences", "d_reader", "USAGE", false},
			}}, nil
		default:
			return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{
				{"d_owner", false, "routines", "public", "EXECUTE", false},
				{"d_owner", false, "tables", "d-dash", "ALL", false},
				{"d_owner", false, "tables", "d_reader", "INSERT", false},
				{"d_owner", false, "tables", "d_reader", "SELECT", false},
			}}, nil
		}
	}
}

// TestReadRevokesFromShow_HappyPath builds one revoke per grantor, class and
// grantee SHOW DEFAULT PRIVILEGES names, asking about every role pg_roles
// lists and about FOR ALL ROLES, one schema at a time.
func TestReadRevokesFromShow_HappyPath(t *testing.T) {
	c := qt.New(t)
	var sent []string
	db := dbtest.Open(c, answerShow(&sent))

	revokes, err := pgdefaultacl.ReadRevokesFromShow(c.Context(), db.SQL, []string{"dapp"})

	c.Assert(err, qt.IsNil)
	c.Assert(revokes, qt.DeepEquals, []pgdefaultacl.Revoke{
		{
			Schema: "dapp", Class: "S", Grantee: "d_reader",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "dapp" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "d_reader"`,
		},
		{
			Schema: "dapp", Grantor: "d_owner", Class: "f",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "dapp" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC`,
		},
		{
			Schema: "dapp", Grantor: "d_owner", Class: "r", Grantee: "d-dash",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "dapp" REVOKE ALL PRIVILEGES ON TABLES FROM "d-dash"`,
		},
		{
			Schema: "dapp", Grantor: "d_owner", Class: "r", Grantee: "d_reader",
			Statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "d_owner" IN SCHEMA "dapp" REVOKE ALL PRIVILEGES ON TABLES FROM "d_reader"`,
		},
	})
	c.Assert(sent, qt.DeepEquals, []string{
		"SELECT rolname FROM pg_roles ORDER BY rolname",
		`SHOW DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA "dapp"`,
		`SHOW DEFAULT PRIVILEGES FOR ROLE "d-dash", "d_owner" IN SCHEMA "dapp"`,
	})
}

// TestReadRevokesFromShow_NoSchemaAsksNothing reads nothing for an empty
// scope, as [pgdefaultacl.ReadRevokes] does.
func TestReadRevokesFromShow_NoSchemaAsksNothing(t *testing.T) {
	c := qt.New(t)
	var sent []string
	db := dbtest.Open(c, answerShow(&sent))

	revokes, err := pgdefaultacl.ReadRevokesFromShow(c.Context(), db.SQL, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(revokes, qt.IsNil)
	c.Assert(sent, qt.HasLen, 0)
}

// TestReadRevokesFromShow_FailurePath refuses an object type IN SCHEMA cannot
// name rather than skipping the grantee, whose default the cleanup would then
// leave behind.
func TestReadRevokesFromShow_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, answerShowSchemas)

	revokes, err := pgdefaultacl.ReadRevokesFromShow(c.Context(), db.SQL, []string{"dapp"})

	c.Assert(err, qt.ErrorMatches,
		`failed to read the default privileges in schema dapp: object type "schemas" is not one IN SCHEMA can name`)
	c.Assert(revokes, qt.IsNil)
}

// answerShowSchemas answers SHOW DEFAULT PRIVILEGES with a SCHEMAS row, which
// only a default set without IN SCHEMA can hold.
func answerShowSchemas(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.HasPrefix(query, "SELECT rolname") {
		return dbtest.QueryResult{Columns: []string{"rolname"}}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{"role", "for_all_roles", "object_type", "grantee", "privilege_type", "is_grantable"},
		Rows:    [][]driver.Value{{nil, true, "schemas", "d_reader", "USAGE", false}},
	}, nil
}
