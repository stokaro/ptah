package mssqlpolicy_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/mssqlpolicy"
)

// TestUnrenderableFor_SeparatesEveryClauseFromItsNeighbour pins which FOR
// clauses SQL Server can express, one operand varied per case.
//
// The pairs matter more than the individual answers. INSERT is expressible with
// a block predicate and not without one, so both spellings appear: a rule
// reading only the operation would accept the filter-only case, and a rule
// reading only the predicate list would refuse the block case. SELECT is
// refused with a block predicate present, because no block predicate fires on a
// read -- that is the case a "does it have a block predicate" rule would let
// through.
func TestUnrenderableFor_SeparatesEveryClauseFromItsNeighbour(t *testing.T) {
	tests := []struct {
		name       string
		policyFor  string
		hasBlock   bool
		renderable bool
	}{
		{name: "unset is every operation", policyFor: "", hasBlock: false, renderable: true},
		{name: "ALL is every operation", policyFor: "ALL", hasBlock: false, renderable: true},
		{name: "ALL with a block predicate", policyFor: "ALL", hasBlock: true, renderable: true},
		{name: "lowercase all is accepted", policyFor: "all", hasBlock: false, renderable: true},
		{name: "surrounding space is trimmed", policyFor: "  ALL  ", hasBlock: false, renderable: true},
		{name: "INSERT rides a block predicate", policyFor: "INSERT", hasBlock: true, renderable: true},
		{name: "UPDATE rides a block predicate", policyFor: "UPDATE", hasBlock: true, renderable: true},
		{name: "DELETE rides a block predicate", policyFor: "DELETE", hasBlock: true, renderable: true},
		{name: "INSERT has nothing to ride", policyFor: "INSERT", hasBlock: false, renderable: false},
		{name: "UPDATE has nothing to ride", policyFor: "UPDATE", hasBlock: false, renderable: false},
		{name: "DELETE has nothing to ride", policyFor: "DELETE", hasBlock: false, renderable: false},
		{name: "SELECT has no form even with a block", policyFor: "SELECT", hasBlock: true, renderable: false},
		{name: "SELECT has no form without one", policyFor: "SELECT", hasBlock: false, renderable: false},
		{name: "an unknown operation is refused", policyFor: "TRUNCATE", hasBlock: true, renderable: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reason := mssqlpolicy.UnrenderableFor(test.policyFor, test.hasBlock)
			if test.renderable {
				c.Assert(reason, qt.Equals, "",
					qt.Commentf("FOR %q (block=%v) is expressible", test.policyFor, test.hasBlock))
				return
			}
			c.Assert(reason, qt.Not(qt.Equals), "",
				qt.Commentf("FOR %q (block=%v) has no T-SQL form", test.policyFor, test.hasBlock))
			// The reason is written into the plan the user reads, so it has to
			// name the clause rather than restate that something is wrong.
			c.Assert(strings.HasSuffix(strings.TrimSpace(reason), "."), qt.IsTrue,
				qt.Commentf("reason is a sentence: %q", reason))
		})
	}
}

// TestUnrenderableFor_DistinguishesTheTwoRefusals keeps the two refusals apart.
//
// They are refused for different reasons and the plan says so: one names a
// clause that needs a block predicate the declaration did not write, the other
// names a clause SQL Server has no form for at all. A single shared message
// would tell a user writing FOR INSERT to give up rather than to add the WITH
// CHECK expression that makes it expressible.
func TestUnrenderableFor_DistinguishesTheTwoRefusals(t *testing.T) {
	c := qt.New(t)

	missingBlock := mssqlpolicy.UnrenderableFor("INSERT", false)
	noFormAtAll := mssqlpolicy.UnrenderableFor("SELECT", false)

	c.Assert(missingBlock, qt.Not(qt.Equals), noFormAtAll)
	c.Assert(missingBlock, qt.Contains, "WITH CHECK")
	c.Assert(noFormAtAll, qt.Contains, "no form for")
}
