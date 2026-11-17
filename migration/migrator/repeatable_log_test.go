package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// logShape is what the log says of one attempt, without the times and the run
// identifier, which differ on every run.
type logShape struct {
	Version    int64
	Repeatable string
	Checksum   string
	Start      migrator.MigrationLogState
	Outcome    migrator.MigrationLogState
}

func logShapes(attempts []migrator.MigrationLogAttempt) []logShape {
	shapes := make([]logShape, 0, len(attempts))
	for _, attempt := range attempts {
		shapes = append(shapes, logShape{
			Version:    attempt.Start.Version,
			Repeatable: attempt.Start.Repeatable,
			Checksum:   attempt.Start.Checksum,
			Start:      attempt.Start.State,
			Outcome:    attempt.Outcome.State,
		})
	}
	return shapes
}

// repeatableLog reads the log of the database at dbPath. Reading the log
// needs no migration files.
func repeatableLog(c *qt.C, dbPath string) []migrator.MigrationLogAttempt {
	c.Helper()
	_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(nil))
	attempts, err := mig.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	return attempts
}

func TestRepeatableMigrationLog_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	fsys := repeatableFiles(map[string]string{
		"R__a_first.sql":    repeatableAuditUp,
		"R__user_names.sql": repeatableViewUp,
	})
	_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
	c.Assert(err, qt.IsNil)
	fsys["R__user_names.sql"].Data = []byte(repeatableViewChangedUp)
	_, err = runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
	c.Assert(err, qt.IsNil)

	attempts := repeatableLog(c, dbPath)

	started, applied := migrator.MigrationLogStarted, migrator.MigrationLogApplied
	c.Assert(logShapes(attempts), qt.DeepEquals, []logShape{
		{Repeatable: "user_names", Checksum: h1(repeatableViewChangedUp), Start: started, Outcome: applied},
		{Repeatable: "user_names", Checksum: h1(repeatableViewUp), Start: started, Outcome: applied},
		{Repeatable: "a_first", Checksum: h1(repeatableAuditUp), Start: started, Outcome: applied},
		{Version: 1, Checksum: attempts[3].Start.Checksum, Start: started, Outcome: applied},
	})
	c.Assert(attempts[1].Start.RunID, qt.Equals, attempts[2].Start.RunID)
	c.Assert(attempts[0].Start.RunID, qt.Not(qt.Equals), attempts[1].Start.RunID)
	c.Assert(attempts[0].Outcome.Repeatable, qt.Equals, "user_names")
}

func TestRepeatableMigrationLog_FailurePath(t *testing.T) {
	// The log lost the outcome of the first repeatable migration and the
	// start of the second, which ran after it in the same run. The outcome
	// left over belongs to the second, so it must not close the first.
	t.Run("entries of two repeatable migrations in one run do not pair with each other", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{
			"R__a_first.sql":    repeatableAuditUp,
			"R__user_names.sql": repeatableViewUp,
		})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		conn, _ := openRepeatableMigrator(c, dbPath, fsys)
		_, err = conn.ExecContext(c.Context(), `DELETE FROM schema_migrations_log
WHERE (repeatable = 'a_first' AND state = 'applied') OR (repeatable = 'user_names' AND state = 'started')`)
		c.Assert(err, qt.IsNil)

		attempts := repeatableLog(c, dbPath)

		c.Assert(attempts, qt.HasLen, 3)
		c.Assert(attempts[0].StartLost(), qt.IsTrue)
		c.Assert(attempts[0].Outcome.Repeatable, qt.Equals, "user_names")
		c.Assert(attempts[1].Undetermined(), qt.IsTrue)
		c.Assert(attempts[1].Start.Repeatable, qt.Equals, "a_first")
	})

	t.Run("a failed attempt is logged as failed, with its error", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))

		attempts := repeatableLog(c, dbPath)

		c.Assert(logShapes(attempts)[0], qt.DeepEquals, logShape{
			Repeatable: "user_names", Checksum: h1(repeatableFailingUp),
			Start: migrator.MigrationLogStarted, Outcome: migrator.MigrationLogFailed,
		})
		c.Assert(attempts[0].Outcome.Error, qt.Matches, `(?s).*THIS IS NOT SQL.*`)
	})

	// The body removes the record before its digest is written, so the run
	// cannot say whether the attempt took effect, and the log says nothing
	// either.
	t.Run("an attempt whose outcome is unknown is undetermined", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		body := "-- +ptah no_transaction\nDROP TABLE schema_migrations_repeatable;\n"
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": body})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		var failure *migrator.RepeatableMigrationError
		c.Assert(err, qt.ErrorAs, &failure)
		c.Assert(failure.OutcomeUnknown, qt.IsTrue)

		attempts := repeatableLog(c, dbPath)

		c.Assert(logShapes(attempts)[0], qt.DeepEquals, logShape{
			Repeatable: "user_names", Checksum: h1(body), Start: migrator.MigrationLogStarted,
		})
		c.Assert(attempts[0].Undetermined(), qt.IsTrue)
	})
}
