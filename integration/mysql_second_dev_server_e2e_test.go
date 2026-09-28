//go:build integration

package integration_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlDevServerEngines pair each MySQL-family server a run reads with a
// second server of the same engine, used as a whole dev server.
var mysqlDevServerEngines = []struct {
	name       string
	admin, dev dbtarget.Engine
}{
	{name: "MySQL", admin: dbtarget.MySQLAdmin, dev: dbtarget.MySQLDevServer},
	{name: "MariaDB", admin: dbtarget.MariaDBAdmin, dev: dbtarget.MariaDBDevServer},
}

// databasesOn answers how many of names the server behind engine holds.
func databasesOn(c *qt.C, engine dbtarget.Engine, names ...string) int {
	c.Helper()
	db, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	count := 0
	for _, name := range names {
		var n int
		c.Assert(db.QueryRowContext(c.Context(),
			"SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&n), qt.IsNil)
		count += n
	}
	return count
}

// TestCompatMigrateDiffReplaysOnASecondDevServerE2E is the control for
// TestCompatMigrateDiffRefusesADevServerThatIsTheDesiredServerE2E: the same
// directory and --to, with a dev server that is another server. The server
// identities differ, so the directory replays on the dev server, the diff
// plans the database --to holds, and the dev server is left empty
// (stokaro/ptah#3885).
func TestCompatMigrateDiffReplaysOnASecondDevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			names := newMySQLServerAccount(c, engine.admin, []string{"app", "second"}, nil)
			desired := newMySQLServerAccount(c, engine.admin, []string{"want"}, []string{"want"})
			dir := devServerMigrationDir(c, names.names["app"], names.names["second"])

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", desired.url,
				"--dev-url", dbtarget.URL(c, engine.dev), "planned")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			planned := readMigrationNamed(c, dir, "planned")
			c.Assert(planned, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+desired.names["want"]+"`")
			c.Assert(planned, qt.Contains, "DROP DATABASE `"+names.names["second"]+"`")
			c.Assert(databasesOn(c, engine.dev, names.names["app"], names.names["second"]), qt.Equals, 0)
		})
	}
}
