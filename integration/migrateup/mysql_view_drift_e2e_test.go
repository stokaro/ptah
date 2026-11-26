//go:build integration

package migrateup_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests run `ptah migrations up --check-drift` on the MySQL family with
// a shadow database whose name differs from the target's. A schema there is a
// database, so a view the migrations create lands in the shadow database under
// the shadow's own name. It is the target's own view, and the check must say so.

// mysqlViewDriftDatabase creates a database of the test's own on the server
// the engine names and returns its address.
func mysqlViewDriftDatabase(c *qt.C, engine dbtarget.Engine, name string) string {
	c.Helper()
	address := dbtarget.URL(c, engine)
	// The server URL of a second dev server names no database, so the
	// database is created through the raw driver.
	conn, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	renamed, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	return renamed
}

// mysqlViewDriftRun writes files into a fresh migrations directory, applies it,
// runs each statement of outOfBand on the target, adds one pending migration,
// and runs the drift check with the shadow.
//
// The drift check trusts the replayed SQL. The histories these tests compare
// create roles, grants, stored bodies and views over another database, which
// the replay guard refuses on a shadow database the run does not own; what
// they measure is the comparison after the replay, and each test drops what
// its history leaves on a server.
func mysqlViewDriftRun(
	c *qt.C, engine mysqlViewDriftEngine, files map[string]string, outOfBand ...string,
) (clirun.Result, migrator.RunResult) {
	c.Helper()
	suffix := time.Now().UnixNano()
	target := mysqlViewDriftDatabase(c, engine.admin, fmt.Sprintf("ptah_view_drift_%d", suffix))
	shadow := mysqlViewDriftDatabase(c, engine.shadowServer, fmt.Sprintf("ptah_view_drift_%d_shadow", suffix))
	dir := c.TempDir()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", target, "--migrations-dir", dir)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
	if len(outOfBand) > 0 {
		conn, err := dbschema.ConnectToDatabase(c.Context(), target)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
		for _, statement := range outOfBand {
			_, err = conn.ExecContext(c.Context(), statement)
			c.Assert(err, qt.IsNil)
		}
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000099_pending.up.sql"),
		[]byte("CREATE TABLE view_drift_pending (id INT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000099_pending.down.sql"),
		[]byte("DROP TABLE view_drift_pending;\n"), 0o600), qt.IsNil)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", target, "--migrations-dir", dir,
		"--check-drift", "--shadow-db", shadow, "--trust-replay-sql", "--json")
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// mysqlViewDriftEngine names the server of the target and the second server
// of the shadow. The shadow is on its own server because GET_LOCK names are
// server-wide: a migrator on a shadow that shares the target's server waits
// on the lock the run holds.
type mysqlViewDriftEngine struct {
	name         string
	admin        dbtarget.Engine
	shadowServer dbtarget.Engine
}

var mysqlViewDriftEngines = []mysqlViewDriftEngine{
	{name: "mysql", admin: dbtarget.MySQLAdmin, shadowServer: dbtarget.MySQLDevServer},
	{name: "mariadb", admin: dbtarget.MariaDBAdmin, shadowServer: dbtarget.MariaDBDevServer},
}

func TestMySQLViewDriftCheckE2E_HappyPath(t *testing.T) {
	const versioned = "-- atlas:txmode none\nCREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
		"CREATE VIEW item_ids AS SELECT id FROM items;\n"
	cases := []struct {
		name            string
		files           map[string]string
		wantRepeatables []string
		wantReplayed    []int64
	}{
		{
			name: "a view a versioned migration creates",
			files: map[string]string{
				"0000000001_items.up.sql":   versioned,
				"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
			},
			wantReplayed: []int64{1},
		},
		{
			name: "a view a repeatable migration creates",
			files: map[string]string{
				"0000000001_items.up.sql":   "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n",
				"0000000001_items.down.sql": "DROP TABLE items;\n",
				"R__item_names.sql":         "CREATE OR REPLACE VIEW item_names AS SELECT name FROM items;\n",
			},
			wantRepeatables: []string{"item_names"},
			wantReplayed:    []int64{1},
		},
		{
			name: "a routine and a trigger a versioned migration creates",
			files: map[string]string{
				"0000000001_items.up.sql": "-- atlas:txmode none\n" +
					"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
					"CREATE FUNCTION item_count() RETURNS INT DETERMINISTIC RETURN 1;\n" +
					"CREATE TRIGGER items_name BEFORE INSERT ON items FOR EACH ROW SET NEW.name = UPPER(NEW.name);\n",
				"0000000001_items.down.sql": "DROP TRIGGER items_name;\nDROP FUNCTION item_count;\nDROP TABLE items;\n",
			},
			wantReplayed: []int64{1},
		},
	}
	for _, engine := range mysqlViewDriftEngines {
		for _, tc := range cases {
			t.Run(engine.name+"/"+tc.name, func(t *testing.T) {
				c := qt.New(t)

				result, document := mysqlViewDriftRun(c, engine, tc.files)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
				c.Assert(document.DriftCheck, qt.IsNotNil)
				c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", document.DriftCheck.Findings))
				c.Assert(document.DriftCheck.Replayed, qt.DeepEquals, tc.wantReplayed)
				c.Assert(document.DriftCheck.ReplayedRepeatables, qt.DeepEquals, tc.wantRepeatables)
				c.Assert(document.DriftCheck.Findings, qt.HasLen, 0)
			})
		}
	}
}

// A view that the target holds and the migrations do not create is still drift,
// and a view whose body differs from the migrated one is too. Without these two
// the happy path would pass for a check that ignores views.
func TestMySQLViewDriftCheckE2E_FailurePath(t *testing.T) {
	files := map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
			"CREATE VIEW item_ids AS SELECT id FROM items;\n",
		"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
	}
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name+"/an extra view on the target", func(t *testing.T) {
			c := qt.New(t)

			result, document := mysqlViewDriftRun(c, engine, files, "CREATE VIEW item_names AS SELECT name FROM items")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(document.DriftCheck.Findings, qt.HasLen, 1)
			c.Assert(document.DriftCheck.Findings[0].Kind, qt.Equals, "extra_view")
			c.Assert(document.DriftCheck.Findings[0].Object, qt.Matches, `ptah_view_drift_\d+\.item_names`)
		})
		t.Run(engine.name+"/a view body changed on the target", func(t *testing.T) {
			c := qt.New(t)

			result, document := mysqlViewDriftRun(c, engine, files, "CREATE OR REPLACE VIEW item_ids AS SELECT id + 1 AS id FROM items")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(document.DriftCheck.Findings, qt.HasLen, 1)
			c.Assert(document.DriftCheck.Findings[0].Kind, qt.Equals, "view_mismatch")
		})
	}
}

// MariaDB records a sequence with the database that holds it, as a view and a
// routine are recorded, so the shadow's own sequence is the target's own.
func TestMariaDBSequenceDriftCheckE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	files := map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY);\nCREATE SEQUENCE item_seq START WITH 5 INCREMENT BY 2;\n",
		"0000000001_items.down.sql": "DROP SEQUENCE item_seq;\nDROP TABLE items;\n",
	}

	result, document := mysqlViewDriftRun(c, mysqlViewDriftEngines[1], files)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
	c.Assert(document.DriftCheck, qt.IsNotNil)
	c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", document.DriftCheck.Findings))
}

// A view in the target's database whose body reads a table of a second database
// names that database in its stored body. The second database exists on both
// servers, as the replay needs it, and the body is compared under its name.
func TestMySQLViewDriftCheckE2E_ReadsAnotherDatabase(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			other := fmt.Sprintf("ptah_view_other_%d", time.Now().UnixNano())
			for _, server := range []dbtarget.Engine{engine.admin, engine.shadowServer} {
				address := mysqlViewDriftDatabase(c, server, other)
				conn, err := dbschema.ConnectToDatabase(c.Context(), address)
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
				_, err = conn.ExecContext(c.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
				c.Assert(err, qt.IsNil)
			}
			files := map[string]string{
				"0000000001_items.up.sql": "-- atlas:txmode none\n" +
					"CREATE VIEW item_ids AS SELECT id FROM " + other + ".t;\n",
				"0000000001_items.down.sql": "DROP VIEW item_ids;\n",
			}

			result, document := mysqlViewDriftRun(c, engine, files)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", document.DriftCheck.Findings))
		})
	}
}
