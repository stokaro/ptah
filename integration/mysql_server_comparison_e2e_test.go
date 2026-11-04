//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema compare` and `schema drift` read a MySQL or MariaDB URL that names
// no database as the whole server, as `schema apply` reads it. The server is
// reached through an account that sees only the databases a test made, so
// the comparison neither sees nor reports a database another test keeps.

// serverComparisonRealm declares the kept database, which the server holds
// with t, and the added database, which it does not hold, with w.
func serverComparisonRealm(kept, added string) string {
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

// TestComparisonVerbsReadAMySQLServerE2E compares the realm with the whole
// server. Both verbs report the database and the table the server lacks,
// drift fails on them, and once `schema apply` has made them, both find the
// server as the realm describes it.
func TestComparisonVerbsReadAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			account := newMySQLServerAccount(c, engine.admin, []string{"kept", "added"}, []string{"kept"})
			kept, added := account.names["kept"], account.names["added"]
			_, err := account.admin.ExecContext(c.Context(), "CREATE TABLE `"+kept+"`.t (id int PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			realm := filepath.Join(c.TempDir(), "realm.hcl")
			c.Assert(os.WriteFile(realm, []byte(serverComparisonRealm(kept, added)), 0o600), qt.IsNil)

			compared := runPtahIn(c, "schema", "compare", "--db-url", account.url, "--schema-file", realm)
			drifted := runPtahIn(c, "schema", "drift", "--db-url", account.url, "--schema-file", realm)
			applied := runPtahIn(c, "schema", "apply", "--db-url", account.url, "--schema-file", realm, "--auto-approve")
			synced := runPtahIn(c, "schema", "compare", "--db-url", account.url, "--schema-file", realm)
			settled := runPtahIn(c, "schema", "drift", "--db-url", account.url, "--schema-file", realm)

			c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
			c.Assert(compared.Stdout, qt.Contains, "schemas_added (1): "+added)
			c.Assert(compared.Stdout, qt.Contains, "tables_added (1): "+added+".w")
			c.Assert(compared.Stdout, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+added+"`")
			c.Assert(compared.Stdout, qt.Not(qt.Contains), "DROP DATABASE")
			c.Assert(drifted.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", drifted.Stdout, drifted.Stderr))
			c.Assert(drifted.Stdout, qt.Contains, "- schemas_added: 1 (safe)")
			c.Assert(drifted.Stdout, qt.Contains, "- tables_added: 1 (safe)")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(account.tables(c, "added"), qt.Equals, "w")
			c.Assert(synced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", synced.Stdout, synced.Stderr))
			c.Assert(synced.Stdout, qt.Not(qt.Contains), "schemas_added")
			c.Assert(synced.Stdout, qt.Not(qt.Contains), "tables_added")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
		})
	}
}
