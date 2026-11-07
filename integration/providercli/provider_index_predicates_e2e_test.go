//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// heldPositiveScores is items with the partial index items_positive on score.
// Its predicate is in the spelling the constraints fixture builds an index
// with and reads it back in: a number quoted, the whole in parentheses.
var heldPositiveScores = []string{`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "score" INTEGER NOT NULL)`,
	`CREATE INDEX "items_positive" ON "items" ("score") WHERE (score > '0')`}

// declaredPositiveScores declares items with items_positive on score under
// the predicate given.
func declaredPositiveScores(predicate string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      score: {type: INTEGER, not_null: true}\n" +
		"    indexes:\n      items_positive: {fields: [score], where: \"" + predicate + "\"}\n"
}

// A predicate the database keeps in a spelling of its own, `(score > '0')`,
// is the declared `score > 0` by the provider's spelling of the declaration,
// and the plan leaves the index. The host's own comparison of predicates reads
// the quoted number as another predicate.
func TestProviderIndexPredicate_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldPositiveScores...)
	f.declare(c, declaredPositiveScores("score > 0"))

	planned := f.plan(c)

	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
	c.Assert(f.storedTable(c, "items_positive"), qt.Equals, heldPositiveScores[1])
}

// A changed predicate rebuilds the index: the plan hands the table to the
// provider, which builds the index with the new predicate in its spelling,
// and a second plan leaves it.
func TestProviderIndexPredicateChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldPositiveScores...)
	f.declare(c, declaredPositiveScores("score > 1"))

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.storedTable(c, "items_positive"), qt.Equals, `CREATE INDEX "items_positive" ON "items" ("score") WHERE (score > '1')`)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// Without the feature the host compares the predicates by its own rules, as it
// does for a provider of an earlier contract, so the index the provider
// rebuilt in its spelling is planned again on every run.
func TestProviderIndexPredicateWithoutTheFeatureNeverConverges(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "unpredicated", heldPositiveScores...)
	f.declare(c, declaredPositiveScores("score > 0"))

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.storedTable(c, "items_positive"), qt.Equals, heldPositiveScores[1])
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
}
