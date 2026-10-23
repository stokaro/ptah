package migrateup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// The reads that run before the migration lock put the schema writer in
// dry-run mode, and the migrator narrates `[DRY RUN] Would initialize
// migrations metadata` for an empty database while it is. A run that did not
// ask for a dry run must not print that record.
func TestMigrateUpRealRunPrintsNoDryRunNarration_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpCapturingStreams(
		"--db-url", "sqlite://"+t.TempDir()+"/real-run-logs.db",
		"--migrations-dir", writeUpMigrations(t),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", stdout, stderr))
	c.Assert(stderr, qt.Not(qt.Contains), "[DRY RUN]")
}
