//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// `migrations down` holds the target's migration lock from its first read
// until it has reported, so it verifies the rollback on its shadow database,
// or derives the rollback there, while it holds that lock. MySQL and MariaDB
// name a lock across the whole server, so a replay on a shadow database of the
// same server that took the migration lock too would wait on the target's
// lock for good. A PostgreSQL advisory lock belongs to one database, which is
// why this needs a MySQL-family server.

// sameServerShadowDowns are the two rollbacks that replay the directory on the
// shadow database: the verification of the down files, and the rollback
// derived by comparison.
var sameServerShadowDowns = []struct {
	name  string
	extra []string
}{
	{name: "verified rollback"},
	{name: "derived rollback", extra: []string{"--plan"}},
}

// writeSameServerShadowMigrations writes two migrations whose down files undo
// them.
func writeSameServerShadowMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000000001_create_users.up.sql":   "CREATE TABLE users (id int PRIMARY KEY);\n",
		"0000000000001_create_users.down.sql": "DROP TABLE users;\n",
		"0000000000002_create_posts.up.sql":   "CREATE TABLE posts (id int PRIMARY KEY);\n",
		"0000000000002_create_posts.down.sql": "DROP TABLE posts;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsDownWithASameServerShadowE2E_HappyPath rolls the target back
// to version 1 with a shadow database on the target's server. The run
// finishes within the timeout and drops posts on the target only.
func TestMigrationsDownWithASameServerShadowE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, down := range sameServerShadowDowns {
			t.Run(engine.name+"/"+down.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				targetName, target := scratch.database(c, "down_target")
				shadowName, shadow := scratch.database(c, "down_shadow")
				dir := writeSameServerShadowMigrations(c)
				options := clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}
				up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
				c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))

				got := clirun.Run(c, clirun.Ptah, options, slices.Concat([]string{
					"migrations", "down", "--db-url", target, "--migrations-dir", dir,
					"--shadow-db", shadow, "--target", "1", "--confirm",
				}, down.extra)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(scratch.tablesOf(c, targetName), qt.Equals, "schema_migrations,schema_migrations_log,users")
				c.Assert(scratch.tablesOf(c, shadowName), qt.Equals, "")
			})
		}
	}
}
