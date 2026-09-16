//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/migration/migrator"
)

// TestMigrationsStatusCheckpointBootstrapE2E reads the document a consumer
// plans from, out of the shipped binary, on both sides of the bootstrap.
//
// stokaro/ptah#3356 is what it refuses: after the bootstrap the aggregate said
// nothing was pending and the records said the two migrations the checkpoint
// replaces were, in the same document. A reader that trusts the records plans
// two migrations the snapshot already created.
//
// SQLite is the engine because the bookkeeping is the subject and it is the
// same on every dialect; what needs a process here is the document the binary
// writes, not the server behind it.
func TestMigrationsStatusCheckpointBootstrapE2E(t *testing.T) {
	c := qt.New(t)

	work := c.TempDir()
	migrations := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000001_widgets.up.sql":               "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"0000000001_widgets.down.sql":             "DROP TABLE widgets;\n",
		"0000000002_color.up.sql":                 "ALTER TABLE widgets ADD COLUMN color TEXT;\n",
		"0000000002_color.down.sql":               "ALTER TABLE widgets DROP COLUMN color;\n",
		"0000000003_snapshot.checkpoint.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY, color TEXT);\n",
		"0000000003_snapshot.checkpoint.down.sql": "DROP TABLE widgets;\n",
		"0000000004_recolor.up.sql":               "UPDATE widgets SET color = 'red';\n",
		"0000000004_recolor.down.sql":             "UPDATE widgets SET color = NULL;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	dbURL := "sqlite://" + filepath.ToSlash(filepath.Join(work, "checkpoint.db"))

	before := statusDocument(c, work, migrations, dbURL)

	c.Assert(before.PendingMigrations, qt.DeepEquals, []int64{3, 4})
	c.Assert(recordStates(before), qt.DeepEquals, []string{
		migrator.MigrationStateCheckpointCovered,
		migrator.MigrationStateCheckpointCovered,
		migrator.MigrationStatePending,
		migrator.MigrationStatePending,
	})

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up", "--migrations-dir", migrations, "--db-url", dbURL)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

	after := statusDocument(c, work, migrations, dbURL)

	// The aggregate and the records answer the same question the same way.
	c.Assert(after.HasPendingChanges, qt.IsFalse)
	c.Assert(after.PendingMigrations, qt.HasLen, 0)
	c.Assert(recordStates(after), qt.DeepEquals, []string{
		migrator.MigrationStateCheckpointCovered,
		migrator.MigrationStateCheckpointCovered,
		migrator.MigrationStateApplied,
		migrator.MigrationStateApplied,
	})
	// And the field a reader uses to tell covered from missing still names the
	// checkpoint that covers them.
	c.Assert(after.CheckpointVersion, qt.Equals, int64(3))
}

func statusDocument(c *qt.C, work, migrations, dbURL string) migrator.MigrationStatus {
	c.Helper()

	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "status", "--migrations-dir", migrations, "--db-url", dbURL, "--json")
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", run.Stderr))

	var status migrator.MigrationStatus
	c.Assert(json.Unmarshal([]byte(run.Stdout), &status), qt.IsNil, qt.Commentf("stdout:\n%s", run.Stdout))
	return status
}

func recordStates(status migrator.MigrationStatus) []string {
	states := make([]string, 0, len(status.Migrations))
	for _, record := range status.Migrations {
		states = append(states, record.State)
	}
	return states
}
