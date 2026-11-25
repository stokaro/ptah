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

// These tests run `ptah migrations drift` over a history that grants
// privileges, on the engines where a schema is a database. The shadow
// database has another name than the target, and a one-database read names
// the database of each grant's object, so the replay's grants name the shadow
// database. They are the target's own grants, and the report must say so.
//
// On MySQL and MariaDB the shadow is on a server of its own, as a shadow on the
// target's server shares the server's roles with it. On ClickHouse it shares
// the target's server, the one server the suite starts: a ClickHouse grant
// names its database, so the shadow's grants and the target's stay apart.

// grantDriftEngine is an engine the report runs on and the way a run is
// prepared there.
type grantDriftEngine struct {
	name   string
	newRun func(c *qt.C) grantDriftRun
}

var grantDriftEngines = []grantDriftEngine{
	{name: "mysql", newRun: func(c *qt.C) grantDriftRun {
		return newMySQLGrantDriftRun(c, dbtarget.MySQLAdmin, dbtarget.MySQLDevServer)
	}},
	{name: "mariadb", newRun: func(c *qt.C) grantDriftRun {
		return newMySQLGrantDriftRun(c, dbtarget.MariaDBAdmin, dbtarget.MariaDBDevServer)
	}},
	{name: "clickhouse", newRun: newClickHouseGrantDriftRun},
}

// grantDriftRun is a target migrated through a history granting role
// privileges on a table, a view and the whole database, and an empty shadow.
// database is the name of the target's database.
type grantDriftRun struct {
	dir, target, shadow, role, database string
}

// grantDriftNames are the role, the target's database and the shadow
// database of one run.
func grantDriftNames() (role, target, shadow string) {
	stamp := time.Now().UnixNano()
	return fmt.Sprintf("ptah_gdrift_%d", stamp),
		fmt.Sprintf("ptah_gdrift_target_%d", stamp),
		fmt.Sprintf("ptah_gdrift_shadow_%d", stamp)
}

// migrate writes the history into a fresh directory and migrates the target
// through it.
func (r *grantDriftRun) migrate(c *qt.C, files map[string]string) {
	c.Helper()
	r.dir = c.TempDir()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(r.dir, name), []byte(body), 0o600), qt.IsNil)
	}
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", r.target, "--migrations-dir", r.dir)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
}

// newMySQLGrantDriftRun creates the target on targetServer and the shadow on
// shadowServer, and migrates the target. The databases and the role are
// dropped from both servers when the test ends.
func newMySQLGrantDriftRun(c *qt.C, targetServer, shadowServer dbtarget.Engine) grantDriftRun {
	c.Helper()
	role, target, shadow := grantDriftNames()
	run := grantDriftRun{role: role, database: target}
	run.target = mysqlGrantDriftDatabase(c, targetServer, target, role)
	run.shadow = mysqlGrantDriftDatabase(c, shadowServer, shadow, role)
	run.migrate(c, map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
			"CREATE VIEW item_ids AS SELECT id FROM items;\n" +
			"CREATE ROLE IF NOT EXISTS " + run.role + ";\n" +
			"GRANT SELECT ON items TO " + run.role + ";\n" +
			"GRANT SELECT ON item_ids TO " + run.role + ";\n" +
			"GRANT SHOW VIEW ON * TO " + run.role + ";\n",
		"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
	})
	return run
}

// mysqlGrantDriftDatabase creates database name on the server engine names and
// returns its address. It drops the database, and role, when the test ends.
func mysqlGrantDriftDatabase(c *qt.C, engine dbtarget.Engine, name, role string) string {
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

// newClickHouseGrantDriftRun creates the target and the shadow on the
// ClickHouse server and migrates the target. The databases and the role are
// dropped when the test ends. A ClickHouse grant on a dropped table outlives
// it, so the role goes as well.
func newClickHouseGrantDriftRun(c *qt.C) grantDriftRun {
	c.Helper()
	role, target, shadow := grantDriftNames()
	run := grantDriftRun{role: role, database: target}
	address := dbtarget.URL(c, dbtarget.ClickHouse)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	urls := make([]string, 0, 2)
	for _, name := range []string{target, shadow} {
		_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
		c.Assert(err, qt.IsNil)
		url, err := atlasurl.WithDatabaseName(address, name)
		c.Assert(err, qt.IsNil)
		urls = append(urls, url)
	}
	c.Cleanup(func() {
		for _, statement := range []string{
			"DROP DATABASE IF EXISTS " + target + " SYNC",
			"DROP DATABASE IF EXISTS " + shadow + " SYNC",
			"DROP ROLE IF EXISTS " + role,
		} {
			_, dropErr := admin.ExecContext(context.Background(), statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("%s", statement))
		}
		dbschema.CloseAndWarn(admin)
	})
	run.target, run.shadow = urls[0], urls[1]
	// INSERT on the whole database stays apart from SELECT on its tables:
	// ClickHouse folds a table grant into a database grant of the same
	// privilege.
	run.migrate(c, map[string]string{
		"0000000001_items.up.sql": "CREATE TABLE items (id Int32, name String) ENGINE = MergeTree ORDER BY id;\n" +
			"CREATE VIEW item_ids AS SELECT id FROM items;\n" +
			"CREATE ROLE IF NOT EXISTS " + run.role + ";\n" +
			"GRANT SELECT ON items TO " + run.role + ";\n" +
			"GRANT SELECT ON item_ids TO " + run.role + ";\n" +
			"GRANT INSERT ON * TO " + run.role + ";\n",
		"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
	})
	return run
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

// report runs the drift report. It trusts the replayed SQL: the history
// creates a role and grants it privileges, which the replay guard refuses on a
// shadow database the run does not own, and the test drops the role itself.
func (r grantDriftRun) report(c *qt.C) (document, clirun.Result) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"migrations", "drift", "--db-url", r.target, "--migrations-dir", r.dir, "--shadow-db", r.shadow,
		"--trust-replay-sql", "--json",
	)
	var got document
	c.Assert(json.Unmarshal([]byte(result.Stdout), &got), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return got, result
}

func TestMigrationsDriftGrantsE2E_HappyPath(t *testing.T) {
	for _, engine := range grantDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := engine.newRun(c)

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", got.Check.Findings))
			c.Assert(got.Check.Findings, qt.HasLen, 0)
			c.Assert(got.Statements, qt.HasLen, 0)
		})
	}
}

// A grant revoked out of band is drift, reported on the target's own table,
// and the statement the report plans restores it on the target: the server
// accepts it, and the next report finds the database matching its history.
func TestMigrationsDriftGrantsE2E_FailurePath(t *testing.T) {
	for _, engine := range grantDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := engine.newRun(c)
			run.exec(c, "REVOKE SELECT ON items FROM "+run.role)

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Check.Findings, qt.DeepEquals, []migrator.DriftFinding{{
				Kind: "missing_grant", Object: run.role + " SELECT ON TABLE " + run.database + ".items",
				Message: "missing grant " + run.role + " SELECT ON TABLE " + run.database + ".items",
			}})
			c.Assert(got.Statements, qt.HasLen, 1)

			run.exec(c, got.Statements...)
			again, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(again.Check.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", again.Check.Findings))
		})
	}
}
