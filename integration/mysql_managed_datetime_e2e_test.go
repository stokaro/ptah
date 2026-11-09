//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// TestMigrationsDataComparesMySQLDateTimeByWallClockE2E checks the shipped
// command with the driver returning text or time.Time in a selected location.
// The changed row also proves that the generated literal is accepted by the
// server's DATETIME column.
func TestMigrationsDataComparesMySQLDateTimeByWallClockE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, databaseURL := scratch.builtFrom(c,
				"CREATE TABLE events (id INT PRIMARY KEY, at DATETIME NOT NULL); "+
					"INSERT INTO events (id, at) VALUES (1, '2026-01-02 03:04:05');")

			root := filepath.Join(c.TempDir(), "entities")
			migrationsDir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
			c.Assert(os.MkdirAll(migrationsDir, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(`package entities

//ptah:schema:data table="events" key="id" file="rows.yaml"
//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="at" type="DATETIME"
	At string
}
`), 0o600), qt.IsNil)
			rowFile := filepath.Join(root, "rows.yaml")
			c.Assert(os.WriteFile(rowFile, []byte("- id: 1\n  at: \"2026-01-02T03:04:05Z\"\n"), 0o600), qt.IsNil)

			for _, setting := range []struct {
				name      string
				parseTime bool
				location  string
			}{
				{name: "driver text"},
				{name: "parsed UTC", parseTime: true, location: "UTC"},
				{name: "parsed Tokyo", parseTime: true, location: "Asia/Tokyo"},
			} {
				t.Run(setting.name, func(t *testing.T) {
					c := qt.New(t)
					scheme, dsn, found := strings.Cut(databaseURL, "://")
					c.Assert(found, qt.IsTrue)
					config, err := mysqldriver.ParseDSN(dsn)
					c.Assert(err, qt.IsNil)
					config.ParseTime = setting.parseTime
					if setting.location != "" {
						config.Loc, err = time.LoadLocation(setting.location)
						c.Assert(err, qt.IsNil)
					}
					address := scheme + "://" + config.FormatDSN()
					result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
						"migrations", "data", "--db-url", address, "--root-dir", root,
						"--migrations-dir", migrationsDir, "--dry-run")
					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stdout, qt.Contains, "no data changes")
				})
			}

			c.Assert(os.WriteFile(rowFile, []byte("- id: 1\n  at: \"2026-01-02T04:04:05Z\"\n"), 0o600), qt.IsNil)
			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
				"migrations", "data", "--db-url", databaseURL, "--root-dir", root,
				"--migrations-dir", migrationsDir, "--allow-destructive", "--dry-run")
			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "SET `at` = '2026-01-02 04:04:05'")
			start := strings.Index(result.Stdout, "UPDATE ")
			c.Assert(start, qt.Not(qt.Equals), -1)
			end := strings.Index(result.Stdout[start:], ";")
			c.Assert(end, qt.Not(qt.Equals), -1)
			statement := result.Stdout[start : start+end+1]
			conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), statement)
			c.Assert(err, qt.IsNil)
			var wallClock string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT DATE_FORMAT(at, '%Y-%m-%d %H:%i:%s') FROM events WHERE id = 1").Scan(&wallClock), qt.IsNil)
			c.Assert(wallClock, qt.Equals, "2026-01-02 04:04:05")
		})
	}
}
