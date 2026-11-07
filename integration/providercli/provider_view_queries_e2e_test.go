//go:build integration

package providercli_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// recentOver declares the table items and the view recent with body.
func recentOver(body string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nviews:\n  recent:\n    body: \"" + body + "\"\n"
}

// exactViewQueries makes the common-objects fixture compare view queries
// itself, keeping the case of every word (schema.view-query-equivalence).
var exactViewQueries = []string{"FIXTURE_VIEW_QUERIES=exact"}

// A provider that compares view queries decides whether a declared query is
// the held one, and the host plans by its answer alone. Its engine keeps the
// case of a name, so a query that changes only the case of a column is
// another query: the view is dropped and created again, and the next plan
// has nothing to change. An unchanged query is the held one, and nothing is
// planned. A provider that does not compare view queries leaves them to the
// host's own reading, which folds case and plans nothing for the same change.
func TestProviderComparesViewQueriesThroughItsOwner_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment []string
		declared    string
		planned     []string
		held        []string
	}{
		{name: "an owner that keeps case replaces a view whose query changes case", environment: exactViewQueries, declared: "SELECT ID FROM items",
			planned: []string{
				`FIXTURE {"op":"drop_view","name":"recent"};`,
				`FIXTURE {"op":"create_view","name":"recent","body":"SELECT ID FROM items"};`,
			}, held: []string{`recent: CREATE VIEW "recent" AS SELECT ID FROM items`}},
		{name: "an owner that keeps case keeps an unchanged view", environment: exactViewQueries, declared: "SELECT  id FROM items",
			held: []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items`}},
		{name: "the host's own reading folds case", declared: "SELECT ID FROM items",
			held: []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
			spec.Environment = append(spec.Environment, test.environment...)
			f := newProviderCLI(c, spec)
			c.Assert(os.WriteFile(f.schemaPath, []byte(recentOver("SELECT id FROM items")), 0o600), qt.IsNil)
			f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
			c.Assert(os.WriteFile(f.schemaPath, []byte(recentOver(test.declared)), 0o600), qt.IsNil)

			planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath)
			f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")

			c.Assert(fixtureStatements.FindAllString(planned, -1), qt.DeepEquals, test.planned)
			_, views := f.keptCommonObjects(c)
			c.Assert(views, qt.DeepEquals, test.held)
			c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
		})
	}
}
