package migratelog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migratelog"
	"ptah.run/migration/migrator"
)

// lostStartFixture is a SQLite database two migrations ran against, whose log
// lost the start of the second, and its URL.
func lostStartFixture(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	ctx := context.Background()
	url := "sqlite://" + filepath.Join(c.TempDir(), "log.db")
	conn, err := dbschema.ConnectToDatabase(ctx, url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_a", "CREATE TABLE a (id INTEGER PRIMARY KEY);\n", "DROP TABLE a;\n"),
		migrator.CreateMigrationFromSQL(2, "create_b", "CREATE TABLE b (id INTEGER PRIMARY KEY);\n", "DROP TABLE b;\n"),
	)).WithActor("release-bot")
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	_, err = conn.ExecContext(ctx, "DELETE FROM schema_migrations_log WHERE version = 2 AND state = 'started'")
	c.Assert(err, qt.IsNil)
	return conn, url
}

// runLog runs the command and returns what it printed.
func runLog(c *qt.C, args ...string) string {
	c.Helper()
	command := migratelog.NewMigrateLogCommand()
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetArgs(args)
	c.Assert(command.ExecuteContext(context.Background()), qt.IsNil)
	return stdout.String()
}

// The table shows the attempt under the version and the actor its outcome names,
// and says nothing about when it began. The row above it is the control: the
// whole attempt beside it still prints its start time.
func TestLogTable_ShowsAnAttemptWhoseStartWasLost(t *testing.T) {
	c := qt.New(t)
	_, url := lostStartFixture(c)

	stdout := runLog(c, "--db-url", url)

	c.Assert(stdout, qt.Matches,
		`STARTED +VERSION +OPERATION +OUTCOME +ACTOR +SOURCE\n`+
			`- +2 +up +applied +release-bot +provided\n`+
			`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z +1 +up +applied +release-bot +provided\n`)
}

// With no other row, the attempt is still what the command reports: a log with
// one row in it has recorded an operation.
func TestLogTable_ReportsAnOutcomeThatIsTheOnlyRow(t *testing.T) {
	c := qt.New(t)
	conn, url := lostStartFixture(c)
	_, err := conn.ExecContext(context.Background(), "DELETE FROM schema_migrations_log WHERE version = 1")
	c.Assert(err, qt.IsNil)

	stdout := runLog(c, "--db-url", url)

	c.Assert(stdout, qt.Matches,
		`STARTED +VERSION +OPERATION +OUTCOME +ACTOR +SOURCE\n- +2 +up +applied +release-bot +provided\n`)
}

// The control for the row above: a table with no rows still says so.
func TestLogTable_SaysSoWhenNothingWasRecorded(t *testing.T) {
	c := qt.New(t)
	conn, url := lostStartFixture(c)
	_, err := conn.ExecContext(context.Background(), "DELETE FROM schema_migrations_log")
	c.Assert(err, qt.IsNil)

	stdout := runLog(c, "--db-url", url)

	c.Assert(stdout, qt.Equals, "No migration operations recorded.\n")
}

// The document leaves started_at out rather than print the zero time, and says
// why, so a consumer does not have to infer it from an absent field.
func TestLogJSON_NamesAnAttemptWhoseStartWasLost(t *testing.T) {
	c := qt.New(t)
	_, url := lostStartFixture(c)

	stdout := runLog(c, "--db-url", url, "--json")

	var records []map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &records), qt.IsNil, qt.Commentf("%s", stdout))
	c.Assert(records, qt.HasLen, 2)
	lost, whole := records[0], records[1]
	c.Assert(lost["start_lost"], qt.IsTrue)
	_, hasStart := lost["started_at"]
	c.Assert(hasStart, qt.IsFalse)
	c.Assert(lost["finished_at"], qt.Not(qt.Equals), nil)
	c.Assert(lost["version"], qt.Equals, float64(2))
	c.Assert(lost["operation"], qt.Equals, "up")
	c.Assert(lost["outcome"], qt.Equals, "applied")
	c.Assert(lost["undetermined"], qt.IsFalse)
	c.Assert(lost["actor"], qt.Equals, "release-bot")
	c.Assert(lost["actor_source"], qt.Equals, "provided")
	c.Assert(lost["run_id"], qt.Equals, whole["run_id"])
	c.Assert(lost["checksum"], qt.Matches, "[0-9a-f]{64}")
	c.Assert(whole["start_lost"], qt.IsFalse)
	c.Assert(whole["started_at"], qt.Not(qt.Equals), nil)
	c.Assert(whole["version"], qt.Equals, float64(1))
}
