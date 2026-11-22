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

// The declared drift check of `ptah-compat migrate apply` replays the applied
// history in the apply's --tx-mode. Each engine's migration 1 holds a
// statement its server refuses inside a transaction block beside one it
// accepts there, so it applies under --tx-mode none and its replay fails in a
// transaction.
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

// TestCompatMigrateApplyDriftCheckReplaysInTheTxModeE2E applies migration 2
// under the mode migration 1 ran in: the replay runs in it, the check is
// clean, and the revision table and the catalog show migration 2.
func TestCompatMigrateApplyDriftCheckReplaysInTheTxModeE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftTxModeProject(c, engine)

			got := compatDriftApply(c, dir, nil, "--tx-mode", "none")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "Drift check: the database matches the schema its 1 applied migration produces\n")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
			c.Assert(pending, qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApplyDriftCheckRefusesInAnotherTxModeE2E is the control: an
// apply in the default mode replays migration 1 in a transaction, the check
// cannot decide, and migration 2 does not run.
func TestCompatMigrateApplyDriftCheckRefusesInAnotherTxModeE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftTxModeProject(c, engine)

			got := compatDriftApply(c, dir, nil)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "the drift check could not compare the database with its applied history")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1"})
			c.Assert(pending, qt.Equals, 0)
		})
	}
}
