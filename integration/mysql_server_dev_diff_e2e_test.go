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

// devDiffRealm declares app, with t(id, name), and more, with m(id).
func devDiffRealm(c *qt.C, app, more string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "realm.hcl")
	c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(`schema %[1]q {}
schema %[2]q {}
table "t" {
  schema = schema.%[1]s
  column "id" {
    null = false
    type = int
  }
  column "name" {
    null = true
    type = varchar(20)
  }
  primary_key {
    columns = [column.id]
  }
}
table "m" {
  schema = schema.%[2]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`, app, more)), 0o600), qt.IsNil)
	return path
}

// devDiffOneSchema declares app alone, with t(id, name).
func devDiffOneSchema(c *qt.C, app string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "one.hcl")
	c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(`schema %[1]q {}
table "t" {
  schema = schema.%[1]s
  column "id" {
    null = false
    type = int
  }
  column "name" {
    null = true
    type = varchar(20)
  }
  primary_key {
    columns = [column.id]
  }
}
`, app)), 0o600), qt.IsNil)
	return path
}

// devDiffMigrationDir writes and hashes a directory creating app with t(id).
func devDiffMigrationDir(c *qt.C, app string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000001_init.sql"), []byte(
		"-- atlas:txmode none\n\nCREATE DATABASE `"+app+"`;\nCREATE TABLE `"+app+"`.t (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// devDiffServer is a server holding app with t(id), seen through an account
// that sees nothing else, and the names more is created under.
func devDiffServer(c *qt.C, engine dbtarget.Engine) mysqlServerAccount {
	c.Helper()
	server := newMySQLServerAccount(c, engine, []string{"app", "more"}, []string{"app"})
	_, err := server.admin.ExecContext(c.Context(), "CREATE TABLE `"+server.names["app"]+"`.t (id int PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	return server
}

// TestSchemaDiffComparesServersOnADevServerE2E diffs whole servers beside a
// whole dev server, as the pinned community binary v1.3.0 does: measured on
// MySQL 8.4.11 and MariaDB 11.8.9, a server, a realm file and a migration
// directory compare database by database, the plan creates and drops
// databases, and the dev server is left empty (stokaro/ptah#3885). The dev
// server is the second server of the engine, because a realm compared with a
// server is created on the dev server, which is refused when it is the server
// read.
func TestSchemaDiffComparesServersOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app, more := server.names["app"], server.names["more"]
			realm := devDiffRealm(c, app, more)
			dev := dbtarget.URL(c, engine.dev)

			toRealm, toRealmErr := runCompatVerb("schema", "diff", "--from", server.url, "--to", "file://"+realm, "--dev-url", dev)
			fromRealm, fromRealmErr := runCompatVerb("schema", "diff", "--from", "file://"+realm, "--to", server.url, "--dev-url", dev)
			fromDir, fromDirErr := runCompatVerb("schema", "diff", "--from", "file://"+devDiffMigrationDir(c, app),
				"--to", "file://"+realm, "--dev-url", dev)
			native, nativeErr := runPtahNativeWithError("schema", "diff", "--from", server.url, "--to", realm, "--dev-url", dev)

			c.Assert(toRealmErr, qt.IsNil, qt.Commentf("%s", toRealm))
			c.Assert(toRealm, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+more+"`")
			c.Assert(toRealm, qt.Contains, "ALTER TABLE `"+app+"`.`t` ADD COLUMN `name`")
			c.Assert(fromRealmErr, qt.IsNil, qt.Commentf("%s", fromRealm))
			c.Assert(fromRealm, qt.Contains, "DROP DATABASE `"+more+"`")
			c.Assert(fromRealm, qt.Contains, "DROP COLUMN `name`")
			c.Assert(fromDirErr, qt.IsNil, qt.Commentf("%s", fromDir))
			c.Assert(fromDir, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+more+"`")
			c.Assert(fromDir, qt.Contains, "ALTER TABLE `"+app+"`.`t` ADD COLUMN `name`")
			c.Assert(nativeErr, qt.IsNil, qt.Commentf("%s", native))
			c.Assert(native, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+more+"`")
			c.Assert(databasesOn(c, engine.dev, app, more), qt.Equals, 0)
		})
	}
}

// TestSchemaDiffComparesOneDatabaseWithOneSchemaOnADevServerE2E is the pair
// the binary compares beside a dev server with one database: an HCL document
// declaring that database alone. The plan stays inside it.
func TestSchemaDiffComparesOneDatabaseWithOneSchemaOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app := server.names["app"]

			out, err := runCompatVerb("schema", "diff", "--from", replaceMySQLDatabaseName(c, server.url, app),
				"--to", "file://"+devDiffOneSchema(c, app), "--dev-url", dbtarget.URL(c, engine.dev))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "ALTER TABLE `"+app+"`.`t` ADD COLUMN `name`")
			c.Assert(out, qt.Not(qt.Contains), "DATABASE")
		})
	}
}

// TestSchemaDiffRefusesOneDatabaseBesideADevServerE2E refuses a database URL
// naming one database beside a realm or SQL on a dev server. Measured on
// MySQL 8.4.11 against the pinned community binary v1.3.0, the binary refuses
// a SQL file or a migration directory there with the sentence pinned below.
// It diffs an HCL realm declaring several databases, and its plan reaches
// databases the one-database side never read: with `more` holding a table,
// the plan's CREATE DATABASE more fails with ERROR 1007 when run, and the
// reverse plans DROP DATABASE more. Ptah refuses that pair with the binary's
// `schema apply` sentence for it (stokaro/ptah#3885).
func TestSchemaDiffRefusesOneDatabaseBesideADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app, more := server.names["app"], server.names["more"]
			one := replaceMySQLDatabaseName(c, server.url, app)
			dev := dbtarget.URL(c, engine.dev)

			toRealm, toRealmErr := runCompatVerb("schema", "diff", "--from", one, "--to", "file://"+devDiffRealm(c, app, more),
				"--dev-url", dev)
			fromRealm, fromRealmErr := runCompatVerb("schema", "diff", "--from", "file://"+devDiffRealm(c, app, more),
				"--to", one, "--dev-url", dev)
			toSQL, toSQLErr := runCompatVerb("schema", "diff", "--from", one,
				"--to", "file://"+devServerSchemaFile(c, app, more), "--dev-url", dev)
			fromDir, fromDirErr := runCompatVerb("schema", "diff", "--from", "file://"+devDiffMigrationDir(c, app),
				"--to", one, "--dev-url", dev)

			c.Assert(toRealmErr, qt.ErrorMatches,
				`(?s).*cannot use HCL with more than 1 schema when --from is limited to schema "`+app+`".*`, qt.Commentf("%s", toRealm))
			c.Assert(fromRealmErr, qt.ErrorMatches,
				`(?s).*cannot use HCL with more than 1 schema when --to is limited to schema "`+app+`".*`, qt.Commentf("%s", fromRealm))
			c.Assert(toSQLErr, qt.ErrorMatches,
				`(?s).*cannot diff a database connection with a schema "`+app+`".*`, qt.Commentf("%s", toSQL))
			c.Assert(fromDirErr, qt.ErrorMatches,
				`(?s).*cannot diff a schema "`+app+`" with a database connection.*`, qt.Commentf("%s", fromDir))
			c.Assert(databasesOn(c, engine.dev, app, more), qt.Equals, 0)
		})
	}
}

// TestSchemaDiffRefusesADevServerThatIsTheServerReadE2E refuses a dev server
// holding a database in the binary's words: here it is the --from server,
// which holds app.
func TestSchemaDiffRefusesADevServerThatIsTheServerReadE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app := server.names["app"]

			out, err := runCompatVerb("schema", "diff", "--from", server.url,
				"--to", "file://"+devDiffRealm(c, app, server.names["more"]), "--dev-url", server.url)

			c.Assert(err, qt.ErrorMatches, `(?s).*connected database is not clean: found schema "`+app+`".*`, qt.Commentf("%s", out))
			c.Assert(server.exists(c, "app"), qt.IsTrue)
		})
	}
}
