package migrationsdrift_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// The report replays the applied history in --tx-mode, as `migrations up
// --check-drift` replays it in the run's. SQLite refuses VACUUM inside a
// transaction, so migration 3 applies under none and its replay fails in a
// transaction.

// vacuumTarget is a target whose migrations 1 to 3 were applied with
// --tx-mode none, migration 3 holding a VACUUM beside another statement.
func vacuumTarget(c *qt.C) target {
	c.Helper()
	tgt := emptyTarget(c)
	tgt.write(c, "0000000003_notes.up.sql", "CREATE TABLE notes (id INTEGER PRIMARY KEY);\nVACUUM;\n")
	tgt.write(c, "0000000003_notes.down.sql", "DROP TABLE notes;\n")
	tgt.up(c, "3", "--tx-mode", "none")
	return tgt
}

// The flag wins over the project's migration.tx_mode, and the project's value
// is the default without it.
func TestMigrationsDrift_HappyPathReplaysInTheTxMode(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		args    []string
	}{
		{name: "the flag", project: "migration:\n  tx_mode: file\n", args: []string{"--tx-mode", "none"}},
		{name: "the project's migration.tx_mode", project: "migration:\n  tx_mode: none\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := vacuumTarget(c)

			got, stderr, code := tgt.document(c, append([]string{"--config", config(c, "ptah.yaml", test.project)}, test.args...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", got.Check))
			c.Assert(got.Check.Replayed, qt.DeepEquals, []int64{1, 2, 3})
			c.Assert(tables(c, tgt.shadowURL), qt.HasLen, 0)
		})
	}
}

// Without --tx-mode the report replays as `migrations up` applies by default,
// in a transaction per file, and a history applied under none fails there:
// the mode is the one the caller states, not one the replay picks.
func TestMigrationsDrift_FailurePathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	tgt := vacuumTarget(c)

	tgt.assertCannotDecide(c, "replay", "replay_error")
}
