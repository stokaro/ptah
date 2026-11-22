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
// drift report replay it in the mode the caller states. The shadow is on a
// server of its own, as GET_LOCK names are server-wide.

// txModeDriftHistory applies migration 1 to a new target with --tx-mode none
// and adds migration 2, which is pending. It returns the directory, the
// target and an empty shadow database on the engine's second server.
func txModeDriftHistory(c *qt.C, engine mysqlViewDriftEngine) (dir, target, shadow string) {
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
	write("0000000002_add_note.up.sql", "ALTER TABLE items ADD COLUMN note varchar(20);\n")
	write("0000000002_add_note.down.sql", "ALTER TABLE items DROP COLUMN note;\n")
	return dir, target, shadow
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

// A history applied under none replays under none: the check is clean and
// migration 2 runs, which the target's catalog shows, and the report agrees.
func TestMySQLDriftCheckTxModeE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name+"/migrations up --check-drift", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := txModeDriftHistory(c, engine)

			result, check := txModeDriftUp(c, "--db-url", target, "--migrations-dir", dir,
				"--tx-mode", "none", "--check-drift", "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(txModeDriftNoteColumns(c, target), qt.Equals, 1)
		})
		t.Run(engine.name+"/migrations drift", func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := txModeDriftHistory(c, engine)

			result, check := txModeDriftReport(c, "--db-url", target, "--migrations-dir", dir,
				"--tx-mode", "none", "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", check))
			c.Assert(check.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(txModeDriftNoteColumns(c, target), qt.Equals, 0)
		})
	}
}

// The replay's mode is the one the caller states, not one the replay picks: a
// report without --tx-mode replays in tx-mode file, where the view is refused.
func TestMySQLDriftCheckTxModeE2E_FailurePath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir, target, shadow := txModeDriftHistory(c, engine)

			result, check := txModeDriftReport(c, "--db-url", target, "--migrations-dir", dir,
				"--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(check.Outcome, qt.Equals, migrator.DriftUnable)
			c.Assert(check.Stage, qt.Equals, "replay")
			c.Assert(check.Message, qt.Contains, "migration 1 cannot run tx-mode file statement 2")
		})
	}
}
