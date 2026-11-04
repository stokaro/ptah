//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A MySQL or MariaDB URL that names no database is the whole server for
// `schema plan` as it is for `schema apply`, and `schema apply --plan` applies
// the plan saved for it. The server is reached through an account that sees
// only the databases a test made, so a plan over the whole server neither
// sees nor drops a database another test keeps.

// serverPlanRealm declares the app database, which the server holds with t1,
// and the added database, which it does not hold.
func serverPlanRealm(app, added string) string {
	return fmt.Sprintf(`schema %[1]q {}
schema %[2]q {}
table "t1" {
  schema = schema.%[1]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "t2" {
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
`, app, added)
}

// serverPlanFixture is a server account seeing app, which holds t1, and added,
// which does not exist yet, with the realm file for both.
type serverPlanFixture struct {
	account mysqlServerAccount
	realm   string
}

func newServerPlanFixture(c *qt.C, engine dbtarget.Engine) serverPlanFixture {
	c.Helper()
	account := newMySQLServerAccount(c, engine, []string{"app", "added"}, []string{"app"})
	_, err := account.admin.ExecContext(c.Context(), "CREATE TABLE `"+account.names["app"]+"`.t1 (id int PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	realm := filepath.Join(c.TempDir(), "realm.hcl")
	c.Assert(os.WriteFile(realm, []byte(serverPlanRealm(account.names["app"], account.names["added"])), 0o600), qt.IsNil)
	return serverPlanFixture{account: account, realm: realm}
}

// plannedStatements are the statements a plan over the fixture holds, in the
// order the server receives them: the tables in the order of their qualified
// names, which puts the added database's first.
func (f serverPlanFixture) plannedStatements() string {
	app, added := f.account.names["app"], f.account.names["added"]
	return "Planned schema changes:\n" +
		"CREATE SCHEMA IF NOT EXISTS `" + added + "`;\n" +
		"CREATE TABLE `" + added + "`.`w` (\n  `id` int PRIMARY KEY\n);\n" +
		"CREATE TABLE `" + app + "`.`t2` (\n  `id` int PRIMARY KEY\n);\n"
}

// TestSchemaPlanReadsAMySQLServerAsApplyDoesE2E plans the realm for the whole
// server, applies the saved plan through the same URL, and reads the databases
// back. The plan lists the statements `schema apply --dry-run` lists for the
// same arguments, and nothing is left to plan afterwards.
func TestSchemaPlanReadsAMySQLServerAsApplyDoesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newServerPlanFixture(c, engine.admin)
			planFile := filepath.Join(c.TempDir(), "server.plan.json")

			dryRun := runPtahNative(c, "schema", "apply", "--db-url", fixture.account.url,
				"--schema-file", fixture.realm, "--dry-run")
			planned := runPtahNative(c, "schema", "plan", "--db-url", fixture.account.url,
				"--schema-file", fixture.realm, "--output", planFile)
			applied := runPtahNative(c, "schema", "apply", "--db-url", fixture.account.url,
				"--plan", planFile, "--auto-approve")
			again := runPtahNative(c, "schema", "plan", "--db-url", fixture.account.url,
				"--schema-file", fixture.realm, "--dry-run")

			c.Assert(dryRun, qt.Contains, fixture.plannedStatements())
			c.Assert(planned, qt.Contains, fixture.plannedStatements())
			c.Assert(applied, qt.Contains, "Schema apply completed successfully.")
			c.Assert(fixture.account.tables(c, "app"), qt.Equals, "t1,t2")
			c.Assert(fixture.account.tables(c, "added"), qt.Equals, "w")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaPlanForAMySQLServerKeepsTheDatabasesSchemasLeavesOutE2E adds a
// database the account sees and the realm does not describe. Planned with
// --schemas naming the realm's databases, the plan does not drop it, and the
// plan applied with --plan leaves it and its table in place, while the
// realm's changes are made.
func TestSchemaPlanForAMySQLServerKeepsTheDatabasesSchemasLeavesOutE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			account := newMySQLServerAccount(c, engine.admin, []string{"app", "added", "other"}, []string{"app", "other"})
			for _, statement := range []string{
				"CREATE TABLE `" + account.names["app"] + "`.t1 (id int PRIMARY KEY)",
				"CREATE TABLE `" + account.names["other"] + "`.o (id int PRIMARY KEY)",
			} {
				_, err := account.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			realm := filepath.Join(c.TempDir(), "realm.hcl")
			c.Assert(os.WriteFile(realm, []byte(serverPlanRealm(account.names["app"], account.names["added"])), 0o600), qt.IsNil)
			planFile := filepath.Join(c.TempDir(), "server.plan.json")

			planned := runPtahNative(c, "schema", "plan", "--db-url", account.url, "--schema-file", realm,
				"--schemas", account.names["app"]+","+account.names["added"], "--output", planFile)
			applied := runPtahNative(c, "schema", "apply", "--db-url", account.url, "--plan", planFile, "--auto-approve")

			c.Assert(planned, qt.Not(qt.Contains), "DROP DATABASE")
			c.Assert(applied, qt.Contains, "Schema apply completed successfully.")
			c.Assert(account.tables(c, "app"), qt.Equals, "t1,t2")
			c.Assert(account.tables(c, "added"), qt.Equals, "w")
			c.Assert(account.tables(c, "other"), qt.Equals, "o")
		})
	}
}

// TestSchemaPlanForAMySQLServerAppliesToThatServerAloneE2E applies the plan
// saved for the whole server through a URL naming its app database. The read
// there is one database, so the plan is refused as stale and nothing runs:
// a plan is applied to the scope it was computed against.
func TestSchemaPlanForAMySQLServerAppliesToThatServerAloneE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newServerPlanFixture(c, engine.admin)
			planFile := filepath.Join(c.TempDir(), "server.plan.json")
			oneDatabase := fixture.account.url + fixture.account.names["app"]

			runPtahNative(c, "schema", "plan", "--db-url", fixture.account.url,
				"--schema-file", fixture.realm, "--output", planFile)
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", oneDatabase,
				"--plan", planFile, "--auto-approve")

			c.Assert(err, qt.ErrorMatches, `pre-planned migration is stale: .*`, qt.Commentf("%s", out))
			c.Assert(fixture.account.tables(c, "app"), qt.Equals, "t1")
			c.Assert(fixture.account.exists(c, "added"), qt.IsFalse)
		})
	}
}

// TestSchemaPlanRefusesADevDatabaseBesideAMySQLServerE2E gives `schema plan`
// a dev URL naming a database beside the whole server. It is refused before
// anything is planned, with the refusal `schema apply` gives the same
// arguments, so no plan is saved for a pair the apply refuses.
func TestSchemaPlanRefusesADevDatabaseBesideAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, verb := range []string{"plan", "apply"} {
			t.Run(engine.name+"/"+verb, func(t *testing.T) {
				c := qt.New(t)
				fixture := newServerPlanFixture(c, engine.admin)
				devDatabase := fixture.account.url + fixture.account.names["app"]

				out, err := runPtahNativeWithError("schema", verb, "--db-url", fixture.account.url,
					"--schema-file", fixture.realm, "--dev-url", devDatabase, "--dry-run")

				c.Assert(err, qt.ErrorMatches, `a dev database beside a whole MySQL or MariaDB server is refused: .*`,
					qt.Commentf("%s", out))
				c.Assert(fixture.account.exists(c, "added"), qt.IsFalse)
			})
		}
	}
}
