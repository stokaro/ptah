package migrationsdrift_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// The report replays each applied file as it ran, as `migrations up
// --check-drift` replays it, and takes no transaction mode. SQLite refuses
// VACUUM inside a transaction, so migration 3 applies under none, and its
// replay runs without a transaction.

// vacuumTarget is a target whose migrations 1 to 3 were applied with
// --tx-mode none, migration 3 holding a VACUUM beside another statement, with
// each statement in outOfBand run on it afterwards.
func vacuumTarget(c *qt.C, outOfBand ...string) target {
	c.Helper()
	tgt := emptyTarget(c)
	tgt.write(c, "0000000003_notes.up.sql", "CREATE TABLE notes (id INTEGER PRIMARY KEY);\nVACUUM;\n")
	tgt.write(c, "0000000003_notes.down.sql", "DROP TABLE notes;\n")
	tgt.up(c, "3", "--tx-mode", "none")
	tgt.exec(c, outOfBand...)
	return tgt
}

// A project's migration.tx_mode, which `migrations up` reads, changes nothing.
func TestMigrationsDrift_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
	}{
		{name: "no project mode", project: "migration: {}\n"},
		{name: "the project's migration.tx_mode", project: "migration:\n  tx_mode: all\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := vacuumTarget(c)

			got, stderr, code := tgt.document(c, "--config", config(c, "ptah.yaml", test.project))

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", got.Check))
			c.Assert(got.Check.Replayed, qt.DeepEquals, []int64{1, 2, 3})
			c.Assert(tables(c, tgt.shadowURL), qt.HasLen, 0)
		})
	}
}

// A table added by hand is drift, and the report has no flag to replay in
// another mode.
func TestMigrationsDrift_FailurePathReplaysEachFileAsItRan(t *testing.T) {
	t.Run("a table added by hand", func(t *testing.T) {
		c := qt.New(t)
		tgt := vacuumTarget(c, "CREATE TABLE extra (id INTEGER PRIMARY KEY)")

		got, stderr, code := tgt.document(c)

		c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
		c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted, qt.Commentf("%+v", got.Check))
		c.Assert(tables(c, tgt.shadowURL), qt.HasLen, 0)
	})
	t.Run("a transaction mode", func(t *testing.T) {
		c := qt.New(t)
		tgt := vacuumTarget(c)

		stdout, stderr, code := tgt.drift(c, "--tx-mode", "none")

		c.Assert(code, qt.Equals, 2)
		c.Assert(stderr, qt.Contains, "unknown flag: --tx-mode")
		c.Assert(stdout, qt.Equals, "")
	})
}
