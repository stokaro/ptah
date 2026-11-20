//go:build integration

package migrationsdrift_test

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

// These tests run `ptah migrations drift` on the MySQL family over a history
// that grants privileges. The shadow database has another name than the
// target, and a one-database read names the database of each grant's object,
// so the replay's grants name the shadow database. They are the target's own
// grants, and the report must say so. The shadow is on a server of its own,
// as a shadow on the target's server shares the server's roles with it.

// grantDriftEngine is a server the target is on and a second server of the
// same engine the shadow is on.
type grantDriftEngine struct {
	name          string
	target, other dbtarget.Engine
}

var grantDriftEngines = []grantDriftEngine{
	{name: "mysql", target: dbtarget.MySQLAdmin, other: dbtarget.MySQLDevServer},
	{name: "mariadb", target: dbtarget.MariaDBAdmin, other: dbtarget.MariaDBDevServer},
}

// grantDriftRun is a target migrated through a history granting role
// privileges on a table, a view and the whole database, and an empty shadow.
type grantDriftRun struct {
	dir, target, shadow, role string
}

// newGrantDriftRun creates the target and the shadow database and migrates
// the target. The databases and the role are dropped from both servers when
// the test ends.
func newGrantDriftRun(c *qt.C, engine grantDriftEngine) grantDriftRun {
	c.Helper()
	stamp := time.Now().UnixNano()
	run := grantDriftRun{dir: c.TempDir(), role: fmt.Sprintf("ptah_gdrift_%d", stamp)}
	run.target = grantDriftDatabase(c, engine.target, fmt.Sprintf("ptah_gdrift_target_%d", stamp), run.role)
	run.shadow = grantDriftDatabase(c, engine.other, fmt.Sprintf("ptah_gdrift_shadow_%d", stamp), run.role)
	files := map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
			"CREATE VIEW item_ids AS SELECT id FROM items;\n" +
			"CREATE ROLE IF NOT EXISTS " + run.role + ";\n" +
			"GRANT SELECT ON items TO " + run.role + ";\n" +
			"GRANT SELECT ON item_ids TO " + run.role + ";\n" +
			"GRANT SHOW VIEW ON * TO " + run.role + ";\n",
		"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(run.dir, name), []byte(body), 0o600), qt.IsNil)
	}
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", run.target, "--migrations-dir", run.dir)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	return run
}

// grantDriftDatabase creates database name on the server engine names and
// returns its address. It drops the database, and role, when the test ends.
func grantDriftDatabase(c *qt.C, engine dbtarget.Engine, name, role string) string {
	c.Helper()
	// The address of a second server names no database, so the database is
	// created through the raw driver.
	conn, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
		c.Check(dropErr, qt.IsNil)
		_, dropErr = conn.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	address, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine), name)
	c.Assert(err, qt.IsNil)
	return address
}

func (r grantDriftRun) exec(c *qt.C, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), r.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

func (r grantDriftRun) report(c *qt.C) (document, clirun.Result) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"migrations", "drift", "--db-url", r.target, "--migrations-dir", r.dir, "--shadow-db", r.shadow, "--json")
	var got document
	c.Assert(json.Unmarshal([]byte(result.Stdout), &got), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return got, result
}

func TestMigrationsDriftGrantsE2E_HappyPath(t *testing.T) {
	for _, engine := range grantDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGrantDriftRun(c, engine)

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", got.Check.Findings))
			c.Assert(got.Check.Findings, qt.HasLen, 0)
			c.Assert(got.Statements, qt.HasLen, 0)
		})
	}
}

// A grant revoked out of band is drift, and the statement the report plans
// restores it on the target: the server accepts it, and the next report finds
// the database matching its history.
func TestMigrationsDriftGrantsE2E_FailurePath(t *testing.T) {
	for _, engine := range grantDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGrantDriftRun(c, engine)
			run.exec(c, "REVOKE SELECT ON items FROM "+run.role)

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Check.Findings, qt.DeepEquals, []migrator.DriftFinding{{
				Kind: "missing_grant", Object: run.role + " SELECT ON TABLE items",
				Message: "missing grant " + run.role + " SELECT ON TABLE items",
			}})
			c.Assert(got.Statements, qt.HasLen, 1)

			run.exec(c, got.Statements...)
			again, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(again.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", again.Check.Findings))
		})
	}
}
