//go:build integration

package providercli_test

import (
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// scopedViewSchema declares the table items, a view recent over it and a role
// reader, followed by grants.
func scopedViewSchema(grants string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nviews:\n  recent:\n    body: \"SELECT id FROM items\"\n" +
		"roles:\n  reader:\n    login: true\n" + grants
}

// recentGrant grants the role reader SELECT on the view recent.
const recentGrant = "grants:\n  reader_recent:\n    role: reader\n    privileges: [select]\n    on_table: recent\n"

// scopedStatements are the statements a plan's text lists, in order.
var scopedStatements = regexp.MustCompile(`(?m)^FIXTURE .*;$`)

// Under --schemas a grant on a view rides the view, as a grant on a table
// rides its table. A declared one is planned and applied, and a held one the
// declaration drops is revoked; each time the next plan has nothing to change.
func TestProviderScopedPlanKeepsViewGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{}))
	for _, run := range []struct {
		name    string
		grants  string
		planned []string
		held    any
	}{
		{name: "the grant declared", grants: recentGrant, planned: []string{
			`FIXTURE {"op":"create_role","kind":"role","name":"reader","login":true};`,
			`FIXTURE {"op":"create_view","name":"recent","body":"SELECT id FROM items"};`,
			`FIXTURE {"op":"grant","kind":"table","name":"recent","member":"reader","privileges":["SELECT"]};`,
		}, held: []any{map[string]any{"role": "reader", "privilege": "SELECT", "kind": "table", "name": "recent"}}},
		{name: "the grant left out", planned: []string{
			`FIXTURE {"op":"revoke","kind":"table","name":"recent","member":"reader","privileges":["SELECT"]};`,
		}},
	} {
		c.Assert(os.WriteFile(f.schemaPath, []byte(scopedViewSchema(run.grants)), 0o600), qt.IsNil)
		planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--schemas", "main")
		f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

		c.Assert(scopedStatements.FindAllString(planned, -1), qt.ContentEquals, run.planned, qt.Commentf("%s", run.name))
		kept, _ := f.keptCommonObjects(c)
		c.Assert(kept["grants"], qt.DeepEquals, run.held, qt.Commentf("%s", run.name))
		c.Assert(f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json", "--schemas", "main"), qt.Contains, `"outcome": "no-changes"`,
			qt.Commentf("%s", run.name))
	}
}
