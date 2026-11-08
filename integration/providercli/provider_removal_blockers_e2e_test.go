//go:build integration

package providercli_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// heldBlockedTables are the tables the removal blocker tests start from: items
// with a label column, and kept.
var heldBlockedTables = []string{
	`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "label" TEXT)`,
	`CREATE TABLE "kept" ("id" INTEGER NOT NULL PRIMARY KEY)`,
}

// The declarations the removal blocker tests plan against heldBlockedTables:
// one that drops items, and one that keeps items without its label column.
const (
	keptOnlySchema       = "tables:\n  kept:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n"
	itemsWithoutLabelSQL = "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n" +
		"  kept:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n"
)

// removalBlockersFixture is the CLI over a fixture database holding
// heldBlockedTables, served by the provider that writes its plans as text and
// whose read reports the removal blockers blockers names, in the form of
// FIXTURE_REMOVAL_BLOCKERS. An empty blockers reports none and lists no
// feature.
func removalBlockersFixture(c *qt.C, blockers string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE=text", "FIXTURE_REMOVAL_BLOCKERS="+blockers)
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	for _, statement := range heldBlockedTables {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return f
}

// A plan that drops a table, or a column, a removal blocker names is refused
// at plan time: the read reports the blocker, the host carries it with the
// held table to the provider's planning service, and the service refuses
// before a statement exists. `schema apply --dry-run` and `migrations
// generate` both refuse, and generate writes no file.
func TestProviderRemovalBlockersRefuseAtPlanTime_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		blockers string
		schema   string
		want     string
	}{
		{name: "a dropped table a blocker names", blockers: "items=stream_one", schema: keptOnlySchema,
			want: "dropping table items: fixture_stream stream_one names it, and the fixture refuses to drop a table one names"},
		{name: "a dropped table a blocker names by a column", blockers: "items=stream_two:label", schema: keptOnlySchema,
			want: "dropping table items: fixture_stream stream_two names it, and the fixture refuses to drop a table one names"},
		{name: "a dropped column a blocker names", blockers: "kept=stream_one;items=stream_two:label", schema: itemsWithoutLabelSQL,
			want: "dropping column label of table items: fixture_stream stream_two names it, and the fixture refuses to drop a column one names"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := removalBlockersFixture(c, test.blockers)
			f.declare(c, test.schema)

			preview, code := f.applyPreview(c)
			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", preview))
			c.Assert(preview, qt.Contains, test.want)

			_, diagnostics, code, dir := f.generateMigration(c)
			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", diagnostics))
			c.Assert(diagnostics, qt.Contains, test.want)
			written, err := filepath.Glob(filepath.Join(dir, "*.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
		})
	}
}

// A blocker blocks only what it names: a table nobody names drops, a column a
// blocker of the whole table does not name drops, and without blockers the
// same plans drop what the refusals above refuse.
func TestProviderRemovalBlockersPlanWhatNoBlockerNames_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		blockers string
		schema   string
		want     string
	}{
		{name: "a table no blocker names", blockers: "kept=stream_one", schema: keptOnlySchema, want: `DROP TABLE "items";`},
		{name: "a column a blocker of the whole table does not name", blockers: "items=stream_one", schema: itemsWithoutLabelSQL,
			want: `ALTER TABLE "items" DROP COLUMN "label";`},
		{name: "a dropped table without blockers", blockers: "", schema: keptOnlySchema, want: `DROP TABLE "items";`},
		{name: "a dropped column without blockers", blockers: "", schema: itemsWithoutLabelSQL, want: `ALTER TABLE "items" DROP COLUMN "label";`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := removalBlockersFixture(c, test.blockers)
			f.declare(c, test.schema)

			preview, code := f.applyPreview(c)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", preview))
			c.Assert(preview, qt.Contains, test.want)
		})
	}
}

// The host carries blockers to planning and nowhere else: a read exported as
// SQL or as HCL names none of them.
func TestProviderRemovalBlockersStayOutOfAnExport_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		args func(providerCLI) []string
		want string
	}{
		{name: "db read", args: func(f providerCLI) []string {
			return []string{"db", "read", "--provider-config", f.manifestPath, "--db-url", f.address}
		}, want: "items"},
		{name: "schema inspect", args: func(f providerCLI) []string { return f.arguments("inspect", "--format", "hcl") }, want: `table "items"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := removalBlockersFixture(c, "items=stream_one;kept=stream_two:id")

			stdout, stderr, code := executeProviderCLI(c.Context(), test.args(f))
			f.assertStopped(c)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, test.want)
			c.Assert(stdout+stderr, qt.Not(qt.Contains), "stream_one")
			c.Assert(stdout+stderr, qt.Not(qt.Contains), "stream_two")
		})
	}
}
