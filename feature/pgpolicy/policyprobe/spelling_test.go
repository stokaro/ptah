package policyprobe_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyprobe"
)

// noSession is a session that cannot open an isolated transaction, so every
// probe is unanswered and the answer comes from what the database holds.
type noSession struct{}

func (noSession) WithRolledBackTransaction(context.Context, string, func(context.Context, *sql.Tx) error) (bool, error) {
	return false, nil
}

// normalizedAgainst puts declared through the service against a database holding
// observed, on table docs, and returns the declaration it hands back.
func normalizedAgainst(c *qt.C, declared *pgpolicy.DesiredPolicy, observed *pgpolicy.ObservedPolicy) *pgpolicy.DesiredPolicy {
	c.Helper()
	ref := pgpolicy.PolicyRef("public", "docs", "p")
	result, err := policyprobe.Service{}.NormalizeObjects(c.Context(), schemaext.NormalizationRequest{
		Target:  "cockroachdb",
		Session: noSession{},
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: declared}))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observed}))},
	})
	c.Assert(err, qt.IsNil)
	object, found, err := result.Desired.Objects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgpolicy.DesiredPolicy)
}

// observedUsing is a policy for every role, on every command, whose USING clause
// the database stores as using.
func observedUsing(using string) *pgpolicy.ObservedPolicy {
	return &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
		Using: &using, Composition: pgpolicy.Permissive}
}

// TestNormalizeObjects_TakesTheObservedSpellingOfTheSameClause holds a declared
// clause beside what CockroachDB v26.3.5 stored for that exact text. Each is one
// condition, so the declaration takes the stored spelling and a comparison finds
// nothing to change.
func TestNormalizeObjects_TakesTheObservedSpellingOfTheSameClause(t *testing.T) {
	tests := []struct{ name, declared, stored string }{
		{"current_user", "owner_name = current_user", "owner_name = current_user()"},
		{"CURRENT_USER", "owner_name = CURRENT_USER", "owner_name = current_user()"},
		{"current_user()", "owner_name = current_user()", "owner_name = current_user()"},
		{"current_role", "owner_name = current_role", "owner_name = current_user()"},
		{"user", "owner_name = user", "owner_name = current_user()"},
		{"session_user", "owner_name = session_user", "owner_name = session_user()"},
		{"current_catalog", "owner_name = current_catalog", "owner_name = current_database()"},
		{"current_schema", "owner_name = current_schema", "owner_name = current_schema()"},
		{"current_date", "created < current_date", "created < current_date()"},
		{"current_timestamp", "created < current_timestamp", "created < current_timestamp():::TIMESTAMPTZ"},
		{"localtimestamp", "created < localtimestamp", "created < localtimestamp():::TIMESTAMP"},
		{"enclosing parentheses", "(owner_name = current_user)", "owner_name = current_user()"},
		{"the table as a prefix", "docs.owner_name = current_user", "owner_name = current_user()"},
		{"a string constant", "owner_name = 'x'", "owner_name = 'x':::STRING"},
		{"a cast of a string to text", "owner_name = 'x'::text", "owner_name = 'x':::STRING"},
		{"a function's constant", "owner_name = current_setting('app.who')", "owner_name = current_setting('app.who':::STRING)"},
		{"an integer list", "n IN (1,2)", "n IN (1:::INT8, 2:::INT8)"},
		{"not equal", "n <> 3", "n != 3:::INT8"},
		{"AND", "tenant = current_setting('app.t', true) AND owner_name = current_user",
			`("tenant" = current_setting('app.t':::STRING, true)) AND (owner_name = current_user())`},
		{"OR", "n > 0 OR n IS NULL", "(n > 0:::INT8) OR (n IS NULL)"},
		{"a chain of AND", "n > 0 AND n < 5 AND n <> 3", "((n > 0:::INT8) AND (n < 5:::INT8)) AND (n != 3:::INT8)"},
		{"AND inside OR", "n > 0 AND n < 5 OR n = 9", "((n > 0:::INT8) AND (n < 5:::INT8)) OR (n = 9:::INT8)"},
		{"OR inside AND", "n > 0 AND (n < 5 OR n = 9)", "(n > 0:::INT8) AND ((n < 5:::INT8) OR (n = 9:::INT8))"},
		{"NOT", "NOT (n > 0)", "NOT (n > 0:::INT8)"},
		{"BETWEEN", "n BETWEEN 1 AND 5", "n BETWEEN 1:::INT8 AND 5:::INT8"},
		{"NOT over BETWEEN", "NOT n BETWEEN 1 AND 5", "NOT (n BETWEEN 1:::INT8 AND 5:::INT8)"},
		{"NOT BETWEEN", "n NOT BETWEEN 1 AND 5", "n NOT BETWEEN 1:::INT8 AND 5:::INT8"},
		{"BETWEEN inside AND", "n BETWEEN 1 AND 5 AND n <> 3", "(n BETWEEN 1:::INT8 AND 5:::INT8) AND (n != 3:::INT8)"},
		{"NOT over BETWEEN inside AND", "NOT (n BETWEEN 1 AND 5) AND n <> 3", "(NOT (n BETWEEN 1:::INT8 AND 5:::INT8)) AND (n != 3:::INT8)"},
		{"CASE", "CASE WHEN n > 0 THEN true ELSE false END", "CASE WHEN n > 0:::INT8 THEN true ELSE false END"},
		{"a function", "lower(owner_name) = 'x'", "lower(owner_name) = 'x':::STRING"},
		{"true", "true", "true"},
		{"a clause declared as the server stores it", "n = (1:::INT8 + 1:::INT8)", "n = (1:::INT8 + 1:::INT8)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &pgpolicy.DesiredPolicy{Using: new(test.declared)}

			got := normalizedAgainst(c, declared, observedUsing(test.stored))

			projected, err := got.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(*projected.Using, qt.Equals, test.stored)
		})
	}
}

// TestNormalizeObjects_KeepsADifferentClauseAsDeclared is the control. Each row is
// a real difference a plan has to make, or a spelling this reading does not
// reconcile, which only costs a repeated ALTER POLICY. The declaration stays as
// written, so a comparison reports it.
func TestNormalizeObjects_KeepsADifferentClauseAsDeclared(t *testing.T) {
	tests := []struct{ name, declared, stored string }{
		{"another keyword", "owner_name = session_user", "owner_name = current_user()"},
		{"another column", "name = current_user", "owner_name = current_user()"},
		{"another string", "owner_name = 'y'", "owner_name = 'x':::STRING"},
		{"a string in another case", "owner_name = 'X'", "owner_name = 'x':::STRING"},
		{"another number", "n > 1", "n > 0:::INT8"},
		{"another operator", "n >= 0", "n > 0:::INT8"},
		{"AND for OR", "n > 0 AND n < 5", "(n > 0:::INT8) OR (n < 5:::INT8)"},
		{"another grouping", "n > 0 AND n < 5 OR n = 9", "(n > 0:::INT8) AND ((n < 5:::INT8) OR (n = 9:::INT8))"},
		{"another operand order", "n < 5 AND n > 0", "(n > 0:::INT8) AND (n < 5:::INT8)"},
		{"a NOT", "NOT (n > 0)", "n > 0:::INT8"},
		{"NOT over another operand", "NOT n > 0 AND n < 5", "NOT ((n > 0:::INT8) AND (n < 5:::INT8))"},
		{"a column in quotes that is not a name unquoted", `"Tenant" = 'x'`, `tenant = 'x':::STRING`},
		{"a column named like a keyword", `"current_user" = 'x'`, `current_user() = 'x':::STRING`},
		{"a keyword as a member of another table", "other.user = 'x'", "current_user() = 'x':::STRING"},
		{"a member named like a keyword", "other.user = 'x'", "other.current_user() = 'x':::STRING"},
		{"a cast of a column", "n::text = 'x'", "n = 'x':::STRING"},
		{"another table's column", "other.n > 0", "n > 0:::INT8"},
		{"a cast to another type", "owner_name = 'x'::bytes", "owner_name = 'x':::STRING"},
		{"arithmetic the server parenthesizes", "n = 1 + 1", "n = (1:::INT8 + 1:::INT8)"},
		{"an array the server rewrites", "owner_name = ANY (ARRAY['a','b'])", "owner_name = ANY ARRAY['a':::STRING, 'b':::STRING]:::STRING[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &pgpolicy.DesiredPolicy{Using: new(test.declared)}

			got := normalizedAgainst(c, declared, observedUsing(test.stored))

			projected, err := got.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(*projected.Using, qt.Equals, test.declared)
		})
	}
}

// TestNormalizeObjects_TakesEachClauseOnItsOwn pins that one clause matching does
// not carry the other: WITH CHECK changed, so it stays as declared while USING
// takes the stored spelling, and a comparison reports only the real change.
func TestNormalizeObjects_TakesEachClauseOnItsOwn(t *testing.T) {
	c := qt.New(t)
	observed := observedUsing("owner_name = current_user()")
	observed.WithCheck = new("n > 0:::INT8")
	declared := &pgpolicy.DesiredPolicy{Using: new("owner_name = current_user"), WithCheck: new("n > 1")}

	got := normalizedAgainst(c, declared, observed)

	projected, err := got.Observed()
	c.Assert(err, qt.IsNil)
	c.Assert(*projected.Using, qt.Equals, "owner_name = current_user()")
	c.Assert(*projected.WithCheck, qt.Equals, "n > 1")
}

// TestNormalizeObjects_LeavesARoleKeywordToTheServer pins that a declaration
// naming a role keyword takes no answer from the database: only a server can say
// which role the keyword resolves to, so the declaration comes back as it was.
func TestNormalizeObjects_LeavesARoleKeywordToTheServer(t *testing.T) {
	c := qt.New(t)
	declared := &pgpolicy.DesiredPolicy{Using: new("owner_name = current_user"),
		Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}}

	got := normalizedAgainst(c, declared, observedUsing("owner_name = current_user()"))

	c.Assert(got.Normalized, qt.IsNil)
}
