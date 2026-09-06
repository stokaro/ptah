package mssqlpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mssqlpolicy"
)

// TestUnrenderableFor_AcceptsEveryClauseTSQLCanCarry pins the expressible side.
//
// INSERT appears here with a WITH CHECK expression and in the refusal table
// without one, because the expression is what decides the answer: it is the
// block predicate the operation rides on. A rule reading only the operation
// would accept both, and a rule reading only the expression would accept
// SELECT, which no block predicate can carry.
func TestUnrenderableFor_AcceptsEveryClauseTSQLCanCarry(t *testing.T) {
	tests := []struct {
		name      string
		policyFor string
		withCheck string
	}{
		{name: "unset is every operation", policyFor: ""},
		{name: "ALL is every operation", policyFor: "ALL"},
		{name: "ALL alongside a block predicate", policyFor: "ALL", withCheck: "dbo.fn_write(tenant)"},
		{name: "lowercase all is accepted", policyFor: "all"},
		{name: "surrounding space is trimmed", policyFor: "  ALL  "},
		{name: "INSERT rides a block predicate", policyFor: "INSERT", withCheck: "dbo.fn_write(tenant)"},
		{name: "UPDATE rides a block predicate", policyFor: "UPDATE", withCheck: "dbo.fn_write(tenant)"},
		{name: "DELETE rides a block predicate", policyFor: "DELETE", withCheck: "dbo.fn_write(tenant)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reason := mssqlpolicy.UnrenderableFor(test.policyFor, test.withCheck)

			c.Assert(reason, qt.Equals, "",
				qt.Commentf("FOR %q with check %q is expressible", test.policyFor, test.withCheck))
		})
	}
}

// TestUnrenderableFor_RefusesEveryClauseTSQLCannot pins the refused side, and
// the fragment each refusal carries.
//
// The two refusals are kept apart on purpose. One names a clause that needs a
// block predicate the declaration did not write; the other names a clause SQL
// Server has no form for at all. A single shared message would tell a user
// writing FOR INSERT to give up rather than to add the WITH CHECK expression
// that makes it expressible.
//
// A whitespace-only expression is refused alongside an absent one: it is not a
// predicate, and treating it as one would render a policy with a block half
// SQL Server would reject.
func TestUnrenderableFor_RefusesEveryClauseTSQLCannot(t *testing.T) {
	tests := []struct {
		name      string
		policyFor string
		withCheck string
		fragment  string
	}{
		{name: "INSERT has nothing to ride", policyFor: "INSERT", fragment: "WITH CHECK"},
		{name: "UPDATE has nothing to ride", policyFor: "UPDATE", fragment: "WITH CHECK"},
		{name: "DELETE has nothing to ride", policyFor: "DELETE", fragment: "WITH CHECK"},
		{name: "a blank expression is not a predicate", policyFor: "UPDATE", withCheck: "   ", fragment: "WITH CHECK"},
		{name: "SELECT has no form without a block", policyFor: "SELECT", fragment: "no form for"},
		{
			name: "SELECT has no form even with one", policyFor: "SELECT",
			withCheck: "dbo.fn_write(tenant)", fragment: "no form for",
		},
		{
			name: "an unknown operation is refused", policyFor: "TRUNCATE",
			withCheck: "dbo.fn_write(tenant)", fragment: "no form for",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reason := mssqlpolicy.UnrenderableFor(test.policyFor, test.withCheck)

			// The reason is written into the plan the user reads, so it has to
			// name the clause rather than restate that something is wrong.
			c.Assert(reason, qt.Contains, test.fragment,
				qt.Commentf("FOR %q with check %q", test.policyFor, test.withCheck))
		})
	}
}
