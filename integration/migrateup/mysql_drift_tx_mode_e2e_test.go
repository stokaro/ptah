//go:build integration

package migrateup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/migration/migrator"
)

// These tests replay, on the MySQL family, a history that `migrations up
// --tx-mode none` applied. Migration 1 creates a view and carries no
// directive, so a run in tx-mode file refuses it, and the drift check and the
// drift report replay it without a transaction, with no mode stated. The
// shadow is on a server of its own, as GET_LOCK names are server-wide.

// txModeDriftHistory applies migration 1 to a new target with --tx-mode none,
// runs each statement of outOfBand on it, and adds migration 2, which is
// pending. It returns the directory, the target and an empty shadow database
// on the engine's second server.
func txModeDriftHistory(c *qt.C, engine mysqlViewDriftEngine, outOfBand ...string) (dir, target, shadow string) {
	c.Helper()
	suffix := time.Now().UnixNano()
	target = mysqlViewDriftDatabase(c, engine.admin, fmt.Sprintf("ptah_txmode_drift_%d", suffix))
	shadow = mysqlViewDriftDatabase(c, engine.shadowServer, fmt.Sprintf("ptah_txmode_drift_%d_shadow", suffix))
	dir = c.TempDir()
	write := func(name, body string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	write("0000000001_create_items.up.sql",
		"CREATE TABLE items (id int PRIMARY KEY, name varchar(50) NOT NULL);\nCREATE VIEW item_names AS SELECT name FROM items;\n")
	write("0000000001_create_items.down.sql", "DROP VIEW item_names;\nDROP TABLE items;\n")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", target, "--migrations-dir", dir, "--tx-mode", "none")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
	for _, statement := range outOfBand {
		txModeDriftExec(c, target, statement)
	}
	write("0000000002_add_note.up.sql", "ALTER TABLE items ADD COLUMN note varchar(20);\n")
	write("0000000002_add_note.down.sql", "ALTER TABLE items DROP COLUMN note;\n")
	return dir, target, shadow
}

// txModeDriftExec runs statement on the target outside any migration.
func txModeDriftExec(c *qt.C, target, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// txModeDriftNoteColumns counts the note columns the target's items table
// holds, which migration 2 adds.
func txModeDriftNoteColumns(c *qt.C, target string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.columns "+
			"WHERE table_schema = DATABASE() AND table_name = 'items' AND column_name = 'note'").Scan(&count), qt.IsNil)
	return count
}

// txModeDriftUp runs `migrations up --json` with args against the history and
// returns the drift check its run document carries.
func txModeDriftUp(c *qt.C, args ...string) (clirun.Result, migrator.DriftCheck) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append([]string{"migrations", "up", "--json"}, args...)...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(document.DriftCheck, qt.IsNotNil, qt.Commentf("%s", result.Stdout))
	return result, *document.DriftCheck
}

// txModeDriftReport runs `migrations drift --json` with args against the
// history and returns the check its report carries.
func txModeDriftReport(c *qt.C, args ...string) (clirun.Result, migrator.DriftCheck) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append([]string{"migrations", "drift", "--json"}, args...)...)
	var document struct {
		Check migrator.DriftCheck `json:"check"`
	}
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document.Check
}

// A history applied under none replays as it ran, whatever mode the run
// applies its pending files in: the check is clean and migration 2 runs,
// which the target's catalog shows, and the report agrees.
func TestMySQLDriftCheckTxModeE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		for _, mode := range []struct {
			name string
			args []string
		}{
			{name: "the default mode"},
			{name: "none", args: []string{"--tx-mode", "none"}},
		} {
			t.Run(engine.name+"/migrations up --check-drift/"+mode.name, func(t *testing.T) {
				c := qt.New(t)
				dir, target, shadow := txModeDriftHistory(c, engine)

				result, check := txModeDriftUp(c, append([]string{"--db-url", target, "--migrations-dir", dir,
					"--check-drift", "--shadow-db", shadow}, mode.args...)...)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
				c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
				c.Assert(check.Replayed, qt.DeepEquals, []int64{1})
				c.Assert(txModeDriftNoteColumns(c, target), qt.Equals, 1)
			})
		}
		t.Run(engine.name+"/migrations drift", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := txModeDriftHistory(c, engine)

			result, check := txModeDriftReport(c, "--db-url", target, "--migrations-dir", dir, "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(txModeDriftNoteColumns(c, target), qt.Equals, 0)
		})
	}
}

// A column added by hand is drift: the replay of the view runs, the report
// names the column, and migration 2 does not run.
func TestMySQLDriftCheckTxModeE2E_FailurePath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := txModeDriftHistory(c, engine, "ALTER TABLE items ADD COLUMN extra int")

			result, check := txModeDriftUp(c, "--db-url", target, "--migrations-dir", dir,
				"--tx-mode", "none", "--check-drift", "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftDrifted, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(txModeDriftNoteColumns(c, target), qt.Equals, 0)
		})
	}
}

// myisamDriftHistory applies a history with a MyISAM table to a new target
// with --tx-mode none and adds migration 3, which is pending. Migration 1
// creates the table; tx-mode file refuses every later file before its first
// statement, since the witness cannot follow a table that is not InnoDB, so
// migration 2 is a file only --tx-mode none applies. Its down file says so,
// as the rollback runs on that database too. It returns the directory, the
// target and an empty shadow database on the engine's second server.
func myisamDriftHistory(c *qt.C, engine mysqlViewDriftEngine) (dir, target, shadow string) {
	c.Helper()
	suffix := time.Now().UnixNano()
	target = mysqlViewDriftDatabase(c, engine.admin, fmt.Sprintf("ptah_myisam_drift_%d", suffix))
	shadow = mysqlViewDriftDatabase(c, engine.shadowServer, fmt.Sprintf("ptah_myisam_drift_%d_shadow", suffix))
	dir = c.TempDir()
	write := func(name, body string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	write("0000000001_mi.up.sql", "CREATE TABLE mi (id int PRIMARY KEY) ENGINE=MyISAM;\nINSERT INTO mi VALUES (1);\n")
	write("0000000001_mi.down.sql", "DROP TABLE mi;\n")
	write("0000000002_z.up.sql", "CREATE TABLE z (id int PRIMARY KEY);\n")
	write("0000000002_z.down.sql", "-- atlas:txmode none\nDROP TABLE z;\n")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", target, "--migrations-dir", dir, "--tx-mode", "none")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
	write("0000000003_note.up.sql", "ALTER TABLE z ADD COLUMN note int;\n")
	write("0000000003_note.down.sql", "-- atlas:txmode none\nALTER TABLE z DROP COLUMN note;\n")
	return dir, target, shadow
}

// myisamDriftCount runs query, a count, on the target.
func myisamDriftCount(c *qt.C, target, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(), query).Scan(&count), qt.IsNil)
	return count
}

const (
	myisamDriftNoteColumns = "SELECT COUNT(*) FROM information_schema.columns " +
		"WHERE table_schema = DATABASE() AND table_name = 'z' AND column_name = 'note'"
	myisamDriftZTables = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'z'"
)

// A history holding a MyISAM table replays: migration 2, which tx-mode file
// refuses for the table migration 1 made, replays without a transaction, with
// no mode stated. The report and the check are clean, the check's run applies
// migration 3 under --tx-mode none, and the rollback rehearsal replays the
// history before it rolls migration 2 back on the target.
func TestMySQLDriftCheckTxModeE2E_MyISAMHistory(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name+"/migrations drift", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := myisamDriftHistory(c, engine)

			result, check := txModeDriftReport(c, "--db-url", target, "--migrations-dir", dir, "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1, 2})
			c.Assert(myisamDriftCount(c, target, myisamDriftNoteColumns), qt.Equals, 0)
		})
		t.Run(engine.name+"/migrations up --check-drift --tx-mode none", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := myisamDriftHistory(c, engine)

			result, check := txModeDriftUp(c, "--db-url", target, "--migrations-dir", dir,
				"--check-drift", "--shadow-db", shadow, "--tx-mode", "none")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1, 2})
			c.Assert(myisamDriftCount(c, target, myisamDriftNoteColumns), qt.Equals, 1)
		})
		t.Run(engine.name+"/migrations down --shadow-db", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := myisamDriftHistory(c, engine)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "migrations", "down", "--db-url", target,
				"--migrations-dir", dir, "--shadow-db", shadow, "--target", "1", "--confirm")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(result.Stdout, qt.Contains, "Rollback plan verified on shadow database")
			c.Assert(myisamDriftCount(c, target, myisamDriftZTables), qt.Equals, 0)
		})
	}
}

// The rule is the replay's alone: migration 3 is pending, and a run in the
// default mode refuses it for the MyISAM table after a clean check, leaving
// the target at version 2.
func TestMySQLDriftCheckTxModeE2E_MyISAMHistoryPendingFile(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := myisamDriftHistory(c, engine)

			result, check := txModeDriftUp(c, "--db-url", target, "--migrations-dir", dir, "--check-drift", "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1, 2})
			c.Assert(result.Stderr, qt.Contains, "tx-mode file requires InnoDB target tables")
			c.Assert(myisamDriftCount(c, target, myisamDriftNoteColumns), qt.Equals, 0)
		})
	}
}
