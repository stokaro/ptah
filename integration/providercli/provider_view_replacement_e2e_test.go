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
// the same change removes goes with the dropped view: it is neither revoked
// nor given again. Either way the next plan has nothing to change.
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

// heldObjects declares the table items, the table archive, a view recent
// over items, the role reader and its grants on recent and archive.
const heldObjects = "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n" +
	"  archive:\n    columns:\n      id: {type: INTEGER, not_null: true}\n" +
	"views:\n  recent:\n    body: \"SELECT id FROM items\"\n" +
	"roles:\n  reader:\n    login: true\n" + readerRecent +
	"  reader_archive:\n    role: reader\n    privileges: [select]\n    on_table: archive\n"

// droppedObjects declares the table items and the role reader alone.
const droppedObjects = "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nroles:\n  reader:\n    login: true\n"

// droppedStatements are the statements a plan's text lists that change the
// fixture's objects, in order.
var droppedStatements = regexp.MustCompile(`(?m)^(FIXTURE |DROP TABLE ).*;$`)

// A table and a view the plan drops take their grants with them: the plan
// drops both and revokes neither grant, the fixture holds no grant once the
// plan applies, and the next plan has nothing to change.
func TestProviderDropsObjectsHoldingGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{}))
	c.Assert(os.WriteFile(f.schemaPath, []byte(heldObjects), 0o600), qt.IsNil)
	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	held, _ := f.keptCommonObjects(c)
	c.Assert(os.WriteFile(f.schemaPath, []byte(droppedObjects), 0o600), qt.IsNil)

	drops := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(held["grants"], qt.HasLen, 2)
	c.Assert(droppedStatements.FindAllString(drops, -1), qt.DeepEquals, []string{`FIXTURE {"op":"drop_view","name":"recent"};`, `DROP TABLE "archive";`})
	kept, views := f.keptCommonObjects(c)
	c.Assert(views, qt.HasLen, 0)
	c.Assert(kept["grants"], qt.IsNil)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// totalsWithAccess declares the table items, a materialized view totals over
// it with body and a role reader, followed by grants.
func totalsWithAccess(body, grants string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nmatviews:\n  totals:\n    body: \"" + body + "\"\n" +
		"roles:\n  reader:\n    login: true\n" + grants
}

// readerTotals grants the role reader SELECT on the materialized view totals.
const readerTotals = "grants:\n  reader_totals:\n    role: reader\n    privileges: [select]\n    on_table: totals\n"

// materializedViews is the environment that makes the common-objects
// fixture carry materialized views.
var materializedViews = []string{"FIXTURE_MATERIALIZED_VIEWS=true"}

// A materialized view the plan drops takes its grants with it, as a view
// does. Removed, it is dropped and the grant on it is not revoked. Changed, it
// is dropped and created again, and the grant it held is given again, unless
// the same change removes the grant. Either way the next plan has nothing to
// change.
func TestProviderDropsAMaterializedViewWithItsGrants_HappyPath(t *testing.T) {
	changed := `FIXTURE {"op":"create_materialized_view","name":"totals","body":"SELECT id FROM items WHERE id \u003e 0"};`
	for _, test := range []struct {
		name     string
		schema   string
		planned  []string
		held     any
		matviews any
	}{
		{name: "removed", schema: droppedObjects, planned: []string{`FIXTURE {"op":"drop_materialized_view","name":"totals"};`}},
		{name: "changed", schema: totalsWithAccess("SELECT id FROM items WHERE id > 0", readerTotals), planned: []string{
			`FIXTURE {"op":"drop_materialized_view","name":"totals"};`, changed,
			`FIXTURE {"op":"grant","kind":"table","name":"totals","member":"reader","privileges":["SELECT"]};`,
		}, held: []any{map[string]any{"role": "reader", "privilege": "SELECT", "kind": "table", "name": "totals"}},
			matviews: []any{map[string]any{"name": "totals", "body": "SELECT id FROM items WHERE id > 0"}}},
		{name: "changed with its grant removed", schema: totalsWithAccess("SELECT id FROM items WHERE id > 0", ""), planned: []string{
			`FIXTURE {"op":"drop_materialized_view","name":"totals"};`, changed,
		}, matviews: []any{map[string]any{"name": "totals", "body": "SELECT id FROM items WHERE id > 0"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
			spec.Environment = append(spec.Environment, materializedViews...)
			f := newProviderCLI(c, spec)
			c.Assert(os.WriteFile(f.schemaPath, []byte(totalsWithAccess("SELECT id FROM items", readerTotals)), 0o600), qt.IsNil)
			f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
			granted, _ := f.keptCommonObjects(c)
			c.Assert(os.WriteFile(f.schemaPath, []byte(test.schema), 0o600), qt.IsNil)

			planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath)
			f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

			c.Assert(granted["grants"], qt.HasLen, 1)
			c.Assert(fixtureStatements.FindAllString(planned, -1), qt.DeepEquals, test.planned)
			kept, _ := f.keptCommonObjects(c)
			c.Assert(kept["grants"], qt.DeepEquals, test.held)
			c.Assert(kept["matviews"], qt.DeepEquals, test.matviews)
			c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
		})
	}
}

// A grant declared on a view or a materialized view the plan removes would
// run after the drop and name an object that is gone, so the plan is refused
// before anything runs: the object stands as it was applied, and a plan of
// that schema has nothing to change. A selection of the view's schema changes
// none of it.
func TestProviderRefusesAGrantOnARemovedView_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment []string
		applied     string
		schema      string
		scope       []string
		want        string
	}{
		{name: "a view in a selected schema", applied: viewWithAccess("SELECT id FROM items", ""), schema: droppedObjects + readerRecent,
			scope: []string{"--schemas", "main"},
			want:  "granting SELECT on view recent to reader: the plan drops the view; declare it, or leave the grant out"},
		{name: "a view", applied: viewWithAccess("SELECT id FROM items", ""), schema: droppedObjects + readerRecent,
			want: "granting SELECT on view recent to reader: the plan drops the view; declare it, or leave the grant out"},
		{name: "a materialized view", environment: materializedViews, applied: totalsWithAccess("SELECT id FROM items", ""), schema: droppedObjects + readerTotals,
			want: "granting SELECT on materialized view totals to reader: the plan drops the materialized view; declare it, or leave the grant out"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
			spec.Environment = append(spec.Environment, test.environment...)
			f := newProviderCLI(c, spec)
			c.Assert(os.WriteFile(f.schemaPath, []byte(test.applied), 0o600), qt.IsNil)
			f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
			c.Assert(os.WriteFile(f.schemaPath, []byte(test.schema), 0o600), qt.IsNil)

			_, refused, code := f.run(c, c.Context(), "apply", append([]string{"--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve"}, test.scope...)...)

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(refused, qt.Contains, test.want)
			c.Assert(os.WriteFile(f.schemaPath, []byte(test.applied), 0o600), qt.IsNil)
			c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
		})
	}
}
