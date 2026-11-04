//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah schema diff` reads a --from MySQL or MariaDB URL that names no
// database as the whole server when the --to side is a file and no --dev-url
// is given, as it reads two servers and as `schema compare` reads one. The
// server is reached through an account that sees only the databases a test
// made.

// serverDiffRealm declares the kept database, which the server holds with t,
// and the added database, which it does not hold, with w. It leaves out the
// gone database, which the server holds with g.
func serverDiffRealm(kept, added string) string {
	return fmt.Sprintf(`schema %[1]q {}
schema %[2]q {}
table "t" {
  schema = schema.%[1]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "w" {
  schema = schema.%[2]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`, kept, added)
}

// TestSchemaDiffReadsAFromServerBesideAFileE2E diffs the whole server to the
// realm. The plan creates the database the server lacks with its table, and
// drops the database the realm leaves out with its table, as a diff of two
// servers does. The reverse direction, a file to the server, has no server to
// read the file on and is refused by name.
func TestSchemaDiffReadsAFromServerBesideAFileE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			account := newMySQLServerAccount(c, engine.admin, []string{"kept", "added", "gone"}, []string{"kept", "gone"})
			kept, added, gone := account.names["kept"], account.names["added"], account.names["gone"]
			for _, statement := range []string{
				"CREATE TABLE `" + kept + "`.t (id int PRIMARY KEY)",
				"CREATE TABLE `" + gone + "`.g (id int PRIMARY KEY)",
			} {
				_, err := account.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			realm := filepath.Join(c.TempDir(), "realm.hcl")
			c.Assert(os.WriteFile(realm, []byte(serverDiffRealm(kept, added)), 0o600), qt.IsNil)

			forward := runPtahIn(c, "schema", "diff", "--from", account.url, "--to", "file://"+realm)
			backward := runPtahIn(c, "schema", "diff", "--from", "file://"+realm, "--to", account.url)

			c.Assert(forward.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", forward.Stdout, forward.Stderr))
			c.Assert(forward.Stdout, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+added+"`;")
			c.Assert(forward.Stdout, qt.Contains, "CREATE TABLE `"+added+"`.`w`")
			c.Assert(forward.Stdout, qt.Contains, "DROP TABLE IF EXISTS `"+gone+"`.`g`;")
			c.Assert(forward.Stdout, qt.Contains, "DROP DATABASE `"+gone+"`;")
			c.Assert(forward.Stdout, qt.Not(qt.Contains), "DROP DATABASE `"+kept+"`")
			c.Assert(backward.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", backward.Stdout))
			c.Assert(backward.Stderr, qt.Contains, "load --to schema: connect to --to database: the database URL names no database")
			c.Assert(account.tables(c, "kept"), qt.Equals, "t")
			c.Assert(account.exists(c, "added"), qt.IsFalse)
			c.Assert(account.tables(c, "gone"), qt.Equals, "g")
		})
	}
}
