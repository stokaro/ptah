//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/migration/migrationfile"
)

// The declared drift check of `ptah-compat migrate apply` replays each applied
// file as it ran, whatever --tx-mode the apply runs its pending files in. Each
// engine's migration 1 holds a statement its server refuses inside a
// transaction block beside one it accepts there, so it applies under
// --tx-mode none, and its replay runs without a transaction.
var compatDriftTxModeFirst = map[string]string{
	"SQLite":     "CREATE TABLE t (id integer PRIMARY KEY);\nVACUUM;\n",
	"PostgreSQL": "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE INDEX CONCURRENTLY t_id ON t (id);\n",
}

// compatDriftTxModeProject is the drift-checked project for engine with
// migration 1 replaced by the engine's entry in compatDriftTxModeFirst, and
// the directory hashed again. Migration 1 is applied with --tx-mode none.
func compatDriftTxModeProject(c *qt.C, engine compatDriftEngine) string {
	c.Helper()
	dir := compatDriftProject(c, engine, compatDriftBlock)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte(compatDriftTxModeFirst[engine.name]), 0o600), qt.IsNil)
	sum, err := atlascompat.ComputeSum(os.DirFS(migrations), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	first := compatDriftApply(c, dir, nil, "1", "--tx-mode", "none")
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	return dir
}

// TestCompatMigrateApplyDriftCheckReplaysEachFileAsItRanE2E applies migration
// 2 in each mode: the check is clean, and the revision table and the catalog
// show migration 2.
func TestCompatMigrateApplyDriftCheckReplaysEachFileAsItRanE2E(t *testing.T) {
	for _, mode := range []struct {
		name string
		args []string
	}{
		{name: "the default mode"},
		{name: "none", args: []string{"--tx-mode", "none"}},
	} {
		for _, engine := range compatDriftEngines(qt.New(t)) {
			t.Run(engine.name+"/"+mode.name, func(t *testing.T) {
				c := qt.New(t)
				dir := compatDriftTxModeProject(c, engine)

				got := compatDriftApply(c, dir, nil, mode.args...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Equals, "Drift check: the database matches the schema its 1 applied migration produces\n")
				versions, pending := compatDriftRead(c, engine)
				c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
				c.Assert(pending, qt.Equals, 1)
			})
		}
	}
}

// TestCompatMigrateApplyDriftCheckRefusesADriftedHistoryE2E adds a column by
// hand: the replay of migration 1 runs, the check finds the column, and
// migration 2 does not run.
func TestCompatMigrateApplyDriftCheckRefusesADriftedHistoryE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftTxModeProject(c, engine)
			compatDriftExec(c, engine.target, "ALTER TABLE t ADD COLUMN extra integer")

			got := compatDriftApply(c, dir, nil, "--tx-mode", "none")

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "Drift check: the database differs from the schema its 1 applied migration produces:\n")
			c.Assert(got.Stderr, qt.Contains, "  - extra column t.extra\n")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1"})
			c.Assert(pending, qt.Equals, 0)
		})
	}
}
