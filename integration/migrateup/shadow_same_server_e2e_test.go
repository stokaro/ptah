//go:build integration

package migrateup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// sameServerDatabases creates two empty databases on engine's server and
// returns their addresses, with a handle on the first.
func sameServerDatabases(c *qt.C, engine dbtarget.Engine) (target, shadow string, conn *dbschema.DatabaseConnection) {
	c.Helper()
	address := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	stamp := time.Now().UnixNano()
	names := []string{fmt.Sprintf("ptah_ss_target_%d", stamp), fmt.Sprintf("ptah_ss_shadow_%d", stamp)}
	urls := make([]string, 0, len(names))
	for _, name := range names {
		_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
		c.Assert(err, qt.IsNil)
		url, err := atlasurl.WithDatabaseName(address, name)
		c.Assert(err, qt.IsNil)
		urls = append(urls, url)
	}
	conn, err = dbschema.ConnectToDatabase(c.Context(), urls[0])
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		for _, name := range names {
			_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	return urls[0], urls[1], conn
}

// tablesIn names the tables of the database url names, sorted.
func tablesIn(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// TestCheckDriftShadowOnTheTargetServerE2E_HappyPath checks the applied
// history in a shadow database on the target's own MySQL or MariaDB server.
// Those engines name an advisory lock for the whole server, and the run holds
// the migration lock while it replays the history, so a replay that took the
// lock as well would wait for the run to end, which it never does. The run is
// bounded, so that wait fails here instead of hanging.
func TestCheckDriftShadowOnTheTargetServerE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, shadow, conn := sameServerDatabases(c, test.engine)
			first := migrationsDir(c, "DROP TABLE created;\n", createCreated)
			second := migrationsDir(c, "DROP TABLE created;\n", createCreated, "CREATE TABLE later (id INT PRIMARY KEY);\n")
			applied := &rollbackTarget{url: target, conn: conn}
			result, document := applied.runUp(c, first)
			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)

			result, document = runUpWithin(c, target, second, time.Minute, "--check-drift", "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(document.Applied, qt.DeepEquals, []int64{2})
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(document.DriftCheck.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(tablesIn(c, target), qt.Contains, "later")
			c.Assert(tablesIn(c, shadow), qt.HasLen, 0)
		})
	}
}

// runUpWithin runs `migrations up --json` against url and stops it after
// timeout.
func runUpWithin(c *qt.C, url, dir string, timeout time.Duration, extra ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	args := append([]string{"migrations", "up", "--db-url", url, "--migrations-dir", dir, "--json"}, extra...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: timeout}, args...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}
