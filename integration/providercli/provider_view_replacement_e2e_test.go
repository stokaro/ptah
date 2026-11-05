//go:build integration

package providercli_test

import (
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// viewWithAccess declares the table items, a view recent over it with body
// and a role reader, followed by grants.
func viewWithAccess(body, grants string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nviews:\n  recent:\n    body: \"" + body + "\"\n" +
		"roles:\n  reader:\n    login: true\n" + grants
}

// readerRecent grants the role reader SELECT on the view recent.
const readerRecent = "grants:\n  reader_recent:\n    role: reader\n    privileges: [select]\n    on_table: recent\n"

// fixtureStatements are the statements a plan's text lists, in order.
var fixtureStatements = regexp.MustCompile(`(?m)^FIXTURE .*;$`)

// A changed view query converges and the view keeps its access. A provider
// whose engine cannot replace a view in place gets the view dropped, created
// again and its grant given again, which the drop took with the view. One
// that replaces in place gets one replacement, which keeps the grant. A grant
// the same change removes is revoked and not given again. Either way the next
// plan has nothing to change.
func TestProviderReplacesAChangedView_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment []string
		grants      string
		planned     []string
		held        any
	}{
		{name: "dropped and created again", grants: readerRecent, planned: []string{
			`FIXTURE {"op":"drop_view","name":"recent"};`,
			`FIXTURE {"op":"create_view","name":"recent","body":"SELECT id FROM items WHERE id \u003e 0"};`,
			`FIXTURE {"op":"grant","kind":"table","name":"recent","member":"reader","privileges":["SELECT"]};`,
		}, held: []any{map[string]any{"role": "reader", "privilege": "SELECT", "kind": "table", "name": "recent"}}},
		{name: "replaced in place", environment: []string{"FIXTURE_CREATE_OR_REPLACE_VIEW=true"}, grants: readerRecent, planned: []string{
			`FIXTURE {"op":"replace_view","name":"recent","body":"SELECT id FROM items WHERE id \u003e 0"};`,
		}, held: []any{map[string]any{"role": "reader", "privilege": "SELECT", "kind": "table", "name": "recent"}}},
		{name: "a grant the change removes", planned: []string{
			`FIXTURE {"op":"revoke","kind":"table","name":"recent","member":"reader","privileges":["SELECT"]};`,
			`FIXTURE {"op":"drop_view","name":"recent"};`,
			`FIXTURE {"op":"create_view","name":"recent","body":"SELECT id FROM items WHERE id \u003e 0"};`,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
			spec.Environment = append(spec.Environment, test.environment...)
			f := newProviderCLI(c, spec)
			c.Assert(os.WriteFile(f.schemaPath, []byte(viewWithAccess("SELECT id FROM items", readerRecent)), 0o600), qt.IsNil)
			f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
			c.Assert(os.WriteFile(f.schemaPath, []byte(viewWithAccess("SELECT id FROM items WHERE id > 0", test.grants)), 0o600), qt.IsNil)

			planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath)
			f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

			c.Assert(fixtureStatements.FindAllString(planned, -1), qt.DeepEquals, test.planned)
			kept, views := f.keptCommonObjects(c)
			c.Assert(views, qt.DeepEquals, []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items WHERE id > 0`})
			c.Assert(kept["grants"], qt.DeepEquals, test.held)
			c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
		})
	}
}
