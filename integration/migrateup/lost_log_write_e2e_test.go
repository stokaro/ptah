//go:build integration

package migrateup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests hold what a caller of `ptah migrations up --json` learns when the
// server refuses a log row. They run the shipped binary because the exit status
// is half the claim, and they read the tables back because the document is a
// report about them.

// tooLongActor is over the actor column's 256 characters on every engine here.
var tooLongActor = strings.Repeat("a", 300)

// lostLogRun is one migrations directory and the tables a run of it writes, on a
// database the test does not own, so every name carries a suffix of its own.
type lostLogRun struct {
	url     string
	conn    *dbschema.DatabaseConnection
	dir     string
	table   string
	objects []string
}

// newLostLogRun writes one migration per body, in order. Each body creates a table
// named for its position, and first runs whatever the test passes for it, with
// {log} standing for the log table's name.
func newLostLogRun(c *qt.C, url string, bodies ...string) *lostLogRun {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	run := &lostLogRun{url: url, conn: conn, dir: c.TempDir(), table: fmt.Sprintf("lost_log_%d", suffix)}
	for index, body := range bodies {
		object := fmt.Sprintf("lost_log_object_%d_%d", suffix, index+1)
		run.objects = append(run.objects, object)
		name := fmt.Sprintf("%010d_step", index+1)
		up := strings.ReplaceAll(body, "{log}", run.table+"_log") +
			fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY);\n", object)
		c.Assert(os.WriteFile(filepath.Join(run.dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(run.dir, name+".down.sql"),
			fmt.Appendf(nil, "DROP TABLE %s;\n", object), 0o600), qt.IsNil)
	}
	c.Cleanup(func() {
		for _, name := range append([]string{run.table, run.table + "_log"}, run.objects...) {
			_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+name)
		}
		dbschema.CloseAndWarn(conn)
	})
	return run
}

// up runs `ptah migrations up --json` and decodes the document.
func (r *lostLogRun) up(c *qt.C, extra ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	args := append([]string{
		"migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir,
		"--migrations-table", r.table, "--json",
	}, extra...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// down runs `ptah migrations down` to version zero, which prints no document.
func (r *lostLogRun) down(c *qt.C, extra ...string) clirun.Result {
	c.Helper()
	args := append([]string{
		"migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir,
		"--migrations-table", r.table, "--target", "0", "--confirm",
	}, extra...)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
}

// logRows reads the log table back as `version:operation:state` in the order the
// run wrote them.
func (r *lostLogRun) logRows(c *qt.C) []string {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(),
		"SELECT version, operation, state FROM "+r.table+"_log ORDER BY logged_at, seq")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var entries []string
	for rows.Next() {
		var (
			version          int64
			operation, state string
		)
		c.Assert(rows.Scan(&version, &operation, &state), qt.IsNil)
		entries = append(entries, strconv.FormatInt(version, 10)+":"+operation+":"+state)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return entries
}

// actors reads back the distinct actors the log table holds.
func (r *lostLogRun) actors(c *qt.C) []string {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(), "SELECT DISTINCT actor FROM "+r.table+"_log")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var actors []string
	for rows.Next() {
		var actor string
		c.Assert(rows.Scan(&actor), qt.IsNil)
		actors = append(actors, actor)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return actors
}

// revisionCount reads how many migrations the revision table records.
func (r *lostLogRun) revisionCount(c *qt.C) int {
	c.Helper()
	var count int
	c.Assert(r.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM "+r.table).Scan(&count), qt.IsNil)
	return count
}

// lostEntries drops the message from each loss so a row can spell the rest, and
// holds the message to naming the server's refusal rather than being empty.
func lostEntries(c *qt.C, report *migrator.MigrationLogReport) []migrator.MigrationLogLoss {
	c.Helper()
	c.Assert(report, qt.IsNotNil)
	entries := make([]migrator.MigrationLogLoss, 0, len(report.Lost))
	for _, loss := range report.Lost {
		c.Assert(loss.Message, qt.Contains, "SQL execution failed")
		loss.Message = ""
		entries = append(entries, loss)
	}
	return entries
}

// loss is the lost entry a row expects, with the codes the engine answers with.
func loss(version int64, state migrator.MigrationLogState, sqlState string, errorNumber int) migrator.MigrationLogLoss {
	return migrator.MigrationLogLoss{
		Operation:   "up",
		Version:     version,
		VersionKey:  strconv.FormatInt(version, 10),
		State:       state,
		SQLState:    sqlState,
		ErrorNumber: errorNumber,
	}
}

// An actor no column can hold loses every entry of the run. The migrations are
// applied, the command exits clean, the document names each entry, and the table
// holds no shortened copy of the name. The rerun is the control: nothing left to
// do, nothing lost, and a document that says so, so a reader can tell it apart.
func TestLongActorLosesTheEntriesAndTheDocumentSaysSoE2E(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		sqlState    string
		errorNumber int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, sqlState: "22001"},
		{name: "mysql", engine: dbtarget.MySQL, sqlState: "22001", errorNumber: 1406},
		{name: "mariadb", engine: dbtarget.MariaDB, sqlState: "22001", errorNumber: 1406},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, dbtarget.URL(c, test.engine), "", "")

			result, document := run.up(c, "--actor", tooLongActor)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(document.Error, qt.IsNil)
			c.Assert(document.Applied, qt.DeepEquals, []int64{1, 2})
			c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
				loss(1, migrator.MigrationLogStarted, test.sqlState, test.errorNumber),
				loss(1, migrator.MigrationLogApplied, test.sqlState, test.errorNumber),
				loss(2, migrator.MigrationLogStarted, test.sqlState, test.errorNumber),
				loss(2, migrator.MigrationLogApplied, test.sqlState, test.errorNumber),
			})
			c.Assert(document.MigrationLog.Written, qt.Equals, 0)
			c.Assert(run.logRows(c), qt.HasLen, 0)
			c.Assert(run.revisionCount(c), qt.Equals, 2)

			again, rerun := run.up(c)

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
			c.Assert(rerun.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
			c.Assert(rerun.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{})
		})
	}
}

// A loss on the second file is reported against the second file. The first file
// plants a constraint the server enforces on the log itself, so the first file's
// entries are in the table and the second file's are not, and the table says so.
// The constraint carries the table's name because MySQL keeps one namespace of
// them per database.
func TestLostEntriesAreReportedAgainstTheirFileE2E(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		sqlState    string
		errorNumber int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, sqlState: "23514"},
		{name: "mysql", engine: dbtarget.MySQL, sqlState: "HY000", errorNumber: 3819},
		{name: "mariadb", engine: dbtarget.MariaDB, sqlState: "23000", errorNumber: 4025},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, dbtarget.URL(c, test.engine),
				"ALTER TABLE {log} ADD CONSTRAINT {log}_refuse CHECK (version <> 2);\n", "")

			result, document := run.up(c, "--actor", "release-bot")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
				loss(2, migrator.MigrationLogStarted, test.sqlState, test.errorNumber),
				loss(2, migrator.MigrationLogApplied, test.sqlState, test.errorNumber),
			})
			c.Assert(document.MigrationLog.Written, qt.Equals, 2)
			c.Assert(run.logRows(c), qt.DeepEquals, []string{"1:up:started", "1:up:applied"})
			c.Assert(run.revisionCount(c), qt.Equals, 2)
		})
	}
}

// A failed run can lose the outcome that records the failure. The document keeps
// the failure and adds the loss, and the exit status stays the failure's. Only
// the start reaches the table, so a reader of the log alone sees an attempt
// nobody can decide.
func TestLostFailureEntryIsReportedE2E(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		sqlState    string
		errorNumber int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, sqlState: "23514"},
		{name: "mysql", engine: dbtarget.MySQL, sqlState: "HY000", errorNumber: 3819},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, dbtarget.URL(c, test.engine),
				"ALTER TABLE {log} ADD CONSTRAINT {log}_refuse CHECK (state <> 'failed');\n",
				"INSERT INTO lost_log_table_nobody_created (id) VALUES (1);\n")

			result, document := run.up(c, "--actor", "release-bot")

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeFailed)
			c.Assert(document.Failure, qt.IsNotNil)
			c.Assert(document.Failure.Version, qt.Equals, int64(2))
			c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
				loss(2, migrator.MigrationLogFailed, test.sqlState, test.errorNumber),
			})
			c.Assert(document.MigrationLog.Written, qt.Equals, 3)
			c.Assert(run.logRows(c), qt.DeepEquals, []string{"1:up:started", "1:up:applied", "2:up:started"})
		})
	}
}

// A character the database's charset cannot hold loses the entries, on a database
// created with that charset. Nothing is written with the character replaced.
func TestCharacterTheCharsetCannotHoldLosesTheEntriesE2E(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		charset     string
		sqlState    string
		errorNumber int
	}{
		{name: "mysql latin1", engine: dbtarget.MySQLAdmin, charset: "latin1", sqlState: "HY000", errorNumber: 3988},
		{name: "mysql utf8mb3", engine: dbtarget.MySQLAdmin, charset: "utf8mb3", sqlState: "HY000", errorNumber: 3988},
		{name: "mariadb latin1", engine: dbtarget.MariaDBAdmin, charset: "latin1", sqlState: "22007", errorNumber: 1366},
		{name: "mariadb utf8mb3", engine: dbtarget.MariaDBAdmin, charset: "utf8mb3", sqlState: "22007", errorNumber: 1366},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, charsetDatabaseURL(c, test.engine, test.charset), "")

			result, document := run.up(c, "--actor", "release-\U0001F680-bot")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
				loss(1, migrator.MigrationLogStarted, test.sqlState, test.errorNumber),
				loss(1, migrator.MigrationLogApplied, test.sqlState, test.errorNumber),
			})
			c.Assert(run.logRows(c), qt.HasLen, 0)
			c.Assert(run.revisionCount(c), qt.Equals, 1)
		})
	}
}

// The control: on the same database a name the charset can hold is stored as it
// was given. The loss above comes from the character and not from the database.
func TestCharacterTheCharsetCanHoldIsStoredE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, charsetDatabaseURL(c, test.engine, "latin1"), "")

			result, document := run.up(c, "--actor", "Müller")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{Written: 2})
			c.Assert(run.actors(c), qt.DeepEquals, []string{"Müller"})
		})
	}
}

// The migration body may create a utf8mb4 table even when the database's
// default charset is latin1. Its checkpoint must accept the same statement.
func TestCheckpointAcceptsUnicodeStatementOnLatin1DatabaseE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name := fmt.Sprintf("unicode_checkpoint_%d", time.Now().UnixNano())
			run := newLostLogRun(c, charsetDatabaseURL(c, test.engine, "latin1"),
				fmt.Sprintf("CREATE TABLE %s (v VARCHAR(20)) CHARACTER SET utf8mb4;\n"+
					"INSERT INTO %s VALUES ('🚀');\n", name, name))

			result, document := run.up(c)
			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(run.revisionCount(c), qt.Equals, 1)

			var value string
			c.Assert(run.conn.QueryRowContext(c.Context(), "SELECT v FROM "+name).Scan(&value), qt.IsNil)
			c.Assert(value, qt.Equals, "🚀")
// A non-strict session must not turn a changed log value into a successful
// write. Strictness applies to the log statement alone: the migration body
// still runs with the mode the caller selected.
func TestNonStrictSessionDoesNotFalsifyMigrationLogE2E(t *testing.T) {
	for _, test := range []struct {
		name         string
		engine       dbtarget.Engine
		charsetState string
		charsetCode  int
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, charsetState: "HY000", charsetCode: 3988},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, charsetState: "22007", charsetCode: 1366},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("long actor", func(t *testing.T) {
				c := qt.New(t)
				name := fmt.Sprintf("non_strict_body_%d", time.Now().UnixNano())
				run := newLostLogRun(c, nonStrictSQLModeURL(c, charsetDatabaseURL(c, test.engine, "latin1")),
					fmt.Sprintf("CREATE TABLE %s (v VARCHAR(3)); INSERT INTO %s VALUES ('abcdef');\n", name, name))

				result, document := run.up(c, "--actor", tooLongActor)
				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
				c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
				c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
					loss(1, migrator.MigrationLogStarted, "22001", 1406),
					loss(1, migrator.MigrationLogApplied, "22001", 1406),
				})
				c.Assert(document.MigrationLog.Written, qt.Equals, 0)
				c.Assert(run.logRows(c), qt.HasLen, 0)
				var bodyValue string
				c.Assert(run.conn.QueryRowContext(c.Context(), "SELECT v FROM "+name).Scan(&bodyValue), qt.IsNil)
				c.Assert(bodyValue, qt.Equals, "abc")
			})
			t.Run("unrepresentable actor", func(t *testing.T) {
				c := qt.New(t)
				run := newLostLogRun(c, nonStrictSQLModeURL(c, charsetDatabaseURL(c, test.engine, "latin1")), "")

				result, document := run.up(c, "--actor", "release-🚀-bot")
				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
				c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
				c.Assert(lostEntries(c, document.MigrationLog), qt.DeepEquals, []migrator.MigrationLogLoss{
					loss(1, migrator.MigrationLogStarted, test.charsetState, test.charsetCode),
					loss(1, migrator.MigrationLogApplied, test.charsetState, test.charsetCode),
				})
				c.Assert(document.MigrationLog.Written, qt.Equals, 0)
				c.Assert(run.logRows(c), qt.HasLen, 0)
			})
		})
	}
}

func nonStrictSQLModeURL(c *qt.C, raw string) string {
	c.Helper()
	scheme, dsn, found := strings.Cut(raw, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	params := make(map[string]string, len(config.Params)+1)
	maps.Copy(params, config.Params)
	config.Params = params
	config.Params["sql_mode"] = "''"
	return scheme + "://" + config.FormatDSN()
}

// `down` prints no document, so its report is the closing notice. The rollback
// ran, the command exits clean, and the table holds no record of it, which is the
// one thing the revision table can no longer say either.
func TestLostRollbackEntriesAreReportedE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL},
		{name: "mysql", engine: dbtarget.MySQL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newLostLogRun(c, dbtarget.URL(c, test.engine), "")
			applied, _ := run.up(c, "--actor", "release-bot")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

			result := run.down(c, "--actor", tooLongActor)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "The migration log lost 2 of 2 entries: version 1 (started, rolled_back).")
			c.Assert(run.logRows(c), qt.DeepEquals, []string{"1:up:started", "1:up:applied"})
			c.Assert(run.revisionCount(c), qt.Equals, 0)
		})
	}
}

// charsetDatabaseURL creates a database with the given default charset, dropped
// when the test ends, and returns the URL ptah connects to it with. It runs as
// the administrator the engine names, because creating a database is what needs
// one.
func charsetDatabaseURL(c *qt.C, engine dbtarget.Engine, charset string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_lost_log_%s_%d", charset, time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name+" CHARACTER SET "+charset)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
		dbschema.CloseAndWarn(admin)
	})
	url, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	return url
}
