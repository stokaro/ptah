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

// `ptah-compat schema plan` computes the plan `schema apply` computes, so a
// MySQL or MariaDB URL that names no database is the whole server for it as
// for apply, and `schema apply --plan` applies the saved plan through the
// same URL. The server is reached through an account that sees only the
// databases a test made, and a `.plan.hcl` plan is verified by a rehearsal on
// a second, whole dev server.

// compatServerPlanRealm declares the kept database, which the server holds
// with t, and the added database, which it does not hold, with w.
func compatServerPlanRealm(kept, added string) string {
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

// compatServerPlanFixture is a server account seeing kept, which holds t, and
// added, which does not exist yet, with the realm file for both.
type compatServerPlanFixture struct {
	account mysqlServerAccount
	realm   string
}

func newCompatServerPlanFixture(c *qt.C, engine dbtarget.Engine) compatServerPlanFixture {
	c.Helper()
	account := newMySQLServerAccount(c, engine, []string{"kept", "added"}, []string{"kept"})
	_, err := account.admin.ExecContext(c.Context(), "CREATE TABLE `"+account.names["kept"]+"`.t (id int PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	realm := filepath.Join(c.TempDir(), "realm.hcl")
	c.Assert(os.WriteFile(realm, []byte(compatServerPlanRealm(account.names["kept"], account.names["added"])), 0o600), qt.IsNil)
	return compatServerPlanFixture{account: account, realm: realm}
}

// TestCompatSchemaPlanReadsAMySQLServerAsApplyDoesE2E plans the realm for the
// whole server and applies the saved plan through the same URL, with a whole
// dev server for the rehearsal that verifies a `.plan.hcl` plan. The plan
// creates the database the server lacks and its table, the databases are read
// back, and the dev server is left without them.
func TestCompatSchemaPlanReadsAMySQLServerAsApplyDoesE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newCompatServerPlanFixture(c, engine.admin)
			added := fixture.account.names["added"]
			planFile := filepath.Join(c.TempDir(), "server.plan.hcl")

			planned, err := runCompatVerb("schema", "plan", "--from", fixture.account.url,
				"--to", "file://"+fixture.realm, "--output", planFile)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))
			applied, err := runCompatVerb("schema", "apply", "--url", fixture.account.url,
				"--plan", "file://"+planFile, "--to", "file://"+fixture.realm,
				"--dev-url", dbtarget.URL(c, engine.dev), "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			again, err := runCompatVerb("schema", "apply", "--url", fixture.account.url,
				"--to", "file://"+fixture.realm, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))

			saved, err := os.ReadFile(planFile)
			c.Assert(err, qt.IsNil)
			c.Assert(string(saved), qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+added+"`;")
			c.Assert(string(saved), qt.Contains, "CREATE TABLE `"+added+"`.`w`")
			c.Assert(string(saved), qt.Not(qt.Contains), "DROP DATABASE")
			c.Assert(fixture.account.tables(c, "kept"), qt.Equals, "t")
			c.Assert(fixture.account.tables(c, "added"), qt.Equals, "w")
			c.Assert(databasesOn(c, engine.dev, fixture.account.names["kept"], added), qt.Equals, 0)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestCompatSchemaPlanRefusesADevDatabaseBesideAMySQLServerE2E gives `schema
// plan` and `schema apply --plan` a dev URL naming a database beside the whole
// server. Each refuses it before contacting it, with the words the community
// binary uses for a document declaring two databases, as the computed apply
// does: the plan creates and drops databases, and a dev database cannot hold
// its rehearsal.
func TestCompatSchemaPlanRefusesADevDatabaseBesideAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newCompatServerPlanFixture(c, engine.admin)
			devDatabase := fixture.account.url + fixture.account.names["kept"]
			planFile := filepath.Join(c.TempDir(), "server.plan.hcl")
			planned, err := runCompatVerb("schema", "plan", "--from", fixture.account.url,
				"--to", "file://"+fixture.realm, "--output", planFile)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))

			plan, planErr := runCompatVerb("schema", "plan", "--from", fixture.account.url,
				"--to", "file://"+fixture.realm, "--dev-url", devDatabase, "--dry-run")
			apply, applyErr := runCompatVerb("schema", "apply", "--url", fixture.account.url,
				"--plan", "file://"+planFile, "--to", "file://"+fixture.realm,
				"--dev-url", devDatabase, "--auto-approve")

			want := `cannot use HCL with more than 1 schema when dev-url is limited to schema "` + fixture.account.names["kept"] + `"`
			c.Assert(planErr, qt.ErrorMatches, want, qt.Commentf("%s", plan))
			c.Assert(applyErr, qt.ErrorMatches, want, qt.Commentf("%s", apply))
			c.Assert(fixture.account.exists(c, "added"), qt.IsFalse)
		})
	}
}
