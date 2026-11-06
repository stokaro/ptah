package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// A SQLite trigger stands in for whatever a server refuses a log row for, and the
// first file plants it, so the refusal starts part-way through one run. A long
// actor would not do here: SQLite stores whatever it is given.
const refuseSecondFileLogRows = `CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE TRIGGER refuse_version_two BEFORE INSERT ON schema_migrations_log
WHEN NEW.version = 2
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END;
`

// writeMigrationsThatLoseTheSecondFile writes two files, the first of which makes
// the log refuse every entry about the second.
func writeMigrationsThatLoseTheSecondFile(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	for name, content := range map[string]string{
		"0000000001_users.up.sql":    refuseSecondFileLogRows,
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// The request: a lost entry is in the document, against the file that lost it,
// the run still reads as what it was, and the command still exits clean.
func TestMigrateUpJSONReportsALostLogEntryAgainstItsFile(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationsThatLoseTheSecondFile(c)

	out, human, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "lost.db"), "--migrations-dir", dir, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.Applied, qt.DeepEquals, []int64{1, 2})
	c.Assert(result.Error, qt.IsNil)
	c.Assert(result.MigrationLog, qt.IsNotNil)
	c.Assert(result.MigrationLog.Written, qt.Equals, 2)
	c.Assert(result.MigrationLog.Lost, qt.HasLen, 2)
	c.Assert(result.MigrationLog.Lost[0].Version, qt.Equals, int64(2))
	c.Assert(result.MigrationLog.Lost[0].VersionKey, qt.Equals, "2")
	c.Assert(result.MigrationLog.Lost[0].Operation, qt.Equals, "up")
	c.Assert(result.MigrationLog.Lost[0].State, qt.Equals, migrator.MigrationLogStarted)
	c.Assert(result.MigrationLog.Lost[0].Message, qt.Contains, "this log row is refused")
	c.Assert(result.MigrationLog.Lost[1].Version, qt.Equals, int64(2))
	c.Assert(result.MigrationLog.Lost[1].State, qt.Equals, migrator.MigrationLogApplied)
	c.Assert(human, qt.Contains, "Could not append to the migration log",
		qt.Commentf("the warning for a person is still written, on the other stream"))
	c.Assert(human, qt.Not(qt.Contains), "The migration log lost",
		qt.Commentf("the closing notice is for a run with no document"))
}

// A failed run keeps its document's failure and adds its lost entries beside it.
func TestMigrateUpJSONReportsALostLogEntryOfAFailedRun(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationsThatLoseTheSecondFile(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_orders.up.sql"), []byte("NOT SQL;\n"), 0o600), qt.IsNil)

	out, human, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "lost.db"), "--migrations-dir", dir, "--json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Failure, qt.IsNotNil)
	c.Assert(result.Failure.Version, qt.Equals, int64(2))
	c.Assert(result.MigrationLog, qt.IsNotNil)
	c.Assert(result.MigrationLog.Written, qt.Equals, 2)
	c.Assert(result.MigrationLog.Lost, qt.HasLen, 2)
	c.Assert(result.MigrationLog.Lost[1].State, qt.Equals, migrator.MigrationLogFailed)
}

// The controls: a run that wrote everything says how much, and a run with
// nothing to do says zero rather than saying nothing. A document that left the
// field out on the up-to-date path would read like a run that keeps no log.
func TestMigrateUpJSONSaysTheLogGotEverything(t *testing.T) {
	c := qt.New(t)
	dir := writeUpMigrations(t)
	dbPath := filepath.Join(c.TempDir(), "clean.db")

	first, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	second, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))

	c.Assert(decodeRunResult(c, first).MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{Written: 4})
	rerun := decodeRunResult(c, second)
	c.Assert(rerun.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
	c.Assert(rerun.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{})
}

// A run that keeps no log has no field at all, whichever setting closed it.
func TestMigrateUpJSONOmitsTheLogWhereNoneIsKept(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "dry run", args: []string{"--dry-run"}},
		{name: "Atlas-compatible revision format", args: []string{"--revision-format", "atlas"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			args := append([]string{
				"--db-url", "sqlite://" + filepath.Join(c.TempDir(), "off.db"),
				"--migrations-dir", writeUpMigrations(t),
				"--json",
			}, test.args...)

			out, human, err := runUpSplit(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
			c.Assert(decodeRunResult(c, out).MigrationLog, qt.IsNil)
		})
	}
}

// The project setting closes the log as the flag does, and the document follows
// it rather than reporting zero rows of a table nothing writes.
func TestMigrateUpJSONOmitsTheLogTheProjectTurnedOff(t *testing.T) {
	c := qt.New(t)
	dir := writeUpMigrations(t)
	dbPath := filepath.Join(c.TempDir(), "project-off.db")
	config := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(config, []byte("migration:\n  log: false\n"), 0o600), qt.IsNil)

	out, human, err := runUpSplit("--config", config, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	c.Assert(decodeRunResult(c, out).MigrationLog, qt.IsNil)
}

// A run that stopped before its migrator ran has nothing to report, so the
// document does not claim a count of zero entries about a run that never started.
func TestMigrateUpJSONOmitsTheLogFromARunThatNeverStarted(t *testing.T) {
	c := qt.New(t)
	dir := writeUpMigrations(t)
	dbPath := filepath.Join(c.TempDir(), "out-of-order.db")
	_, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	// Below the applied versions, which the default linear order refuses before
	// the migrator selects anything.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000000_early.up.sql"), []byte("CREATE TABLE early (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000000_early.down.sql"), []byte("DROP TABLE early;\n"), 0o600), qt.IsNil)

	out, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.MigrationLog, qt.IsNil)
}

// A person reads the last lines. They say the run succeeded, so the loss has to
// come after them.
func TestMigrateUpTellsAPersonWhatTheLogLost(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationsThatLoseTheSecondFile(c)

	out, human, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "lost.db"), "--migrations-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	c.Assert(out, qt.Matches, `(?s).*Migrations completed successfully.*The migration log lost 2 of 4 entries: version 2 \(started, applied\)\..*`)
}

// The notice follows a failure too, ahead of the error the command returns.
func TestMigrateUpTellsAPersonWhatAFailedRunLost(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationsThatLoseTheSecondFile(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_orders.up.sql"), []byte("NOT SQL;\n"), 0o600), qt.IsNil)

	out, human, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "lost.db"), "--migrations-dir", dir)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", human))
	c.Assert(out, qt.Contains, "The migration log lost 2 of 4 entries: version 2 (started, failed).")
}

// The control: a run that lost nothing says nothing about the log.
func TestMigrateUpSaysNothingOfALogThatLostNothing(t *testing.T) {
	c := qt.New(t)

	out, human, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "clean.db"), "--migrations-dir", writeUpMigrations(t))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	c.Assert(out, qt.Contains, "Migrations completed successfully")
	c.Assert(out, qt.Not(qt.Contains), "migration log")
}
