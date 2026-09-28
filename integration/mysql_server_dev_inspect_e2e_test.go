//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// devServerSchemaFile writes a SQL schema file for a whole server: app with t,
// and other with x and a foreign key into app.t.
func devServerSchemaFile(c *qt.C, app, other string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE DATABASE `"+app+"`;\n"+
		"CREATE TABLE `"+app+"`.t (id int PRIMARY KEY);\n"+
		"CREATE DATABASE `"+other+"`;\n"+
		"CREATE TABLE `"+other+"`.x (id int PRIMARY KEY, t_id int, "+
		"CONSTRAINT x_t FOREIGN KEY (t_id) REFERENCES `"+app+"`.t (id));\n"), 0o600), qt.IsNil)
	return path
}

// databasesLeft answers how many of names the server holds, as its
// administrator sees it.
func (s mysqlServerAccount) databasesLeft(c *qt.C, names ...string) int {
	c.Helper()
	count := 0
	for _, name := range names {
		var n int
		c.Assert(s.admin.QueryRowContext(c.Context(),
			"SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&n), qt.IsNil)
		count += n
	}
	return count
}

// TestSchemaInspectMaterializesOnADevServerE2E inspects a schema file and a
// migration directory on a whole dev server, a --dev-url naming no database,
// as the pinned community binary v1.3.0 does: measured on MySQL 8.4.11 and
// MariaDB 11.8.9, it creates the databases the source declares, prints them
// with their tables, and leaves the dev server empty (stokaro/ptah#3885).
// Both sources keep a foreign key from one database into another, which the
// binary's cleanup stops on with error 3730 or 1451, leaving the databases
// behind. The dev server is the shared CI server seen through an account that
// sees no database.
func TestSchemaInspectMaterializesOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "second", "third"}, nil)
			app, second, third := server.names["app"], server.names["second"], server.names["third"]

			fromSQL, sqlErr := runCompatVerb("schema", "inspect", "-u", "file://"+devServerSchemaFile(c, app, second),
				"--dev-url", server.url, "--format", "{{ sql . }}")
			fromHCL, hclErr := runCompatVerb("schema", "inspect", "-u", "file://"+devServerRealm(c, app, third),
				"--dev-url", server.url, "--format", "{{ sql . }}")
			fromDir, dirErr := runCompatVerb("schema", "inspect", "-u", "file://"+devServerMigrationDir(c, app, second),
				"--dev-url", server.url, "--format", "{{ sql . }}")
			native, nativeErr := runPtahNativeWithError("schema", "inspect",
				"--schema-file", devServerSchemaFile(c, app, second), "--dev-url", server.url, "--format", "sql")

			c.Assert(sqlErr, qt.IsNil, qt.Commentf("%s", fromSQL))
			c.Assert(fromSQL, qt.Contains, "CREATE TABLE `"+app+"`.`t`")
			c.Assert(fromSQL, qt.Contains, "REFERENCES `"+app+"`.`t`")
			c.Assert(hclErr, qt.IsNil, qt.Commentf("%s", fromHCL))
			c.Assert(fromHCL, qt.Contains, "CREATE TABLE `"+third+"`.`w`")
			c.Assert(dirErr, qt.IsNil, qt.Commentf("%s", fromDir))
			c.Assert(fromDir, qt.Contains, "CREATE TABLE `"+second+"`.`x`")
			c.Assert(nativeErr, qt.IsNil, qt.Commentf("%s", native))
			c.Assert(native, qt.Contains, "REFERENCES `"+app+"`.`t`")
			c.Assert(server.databasesLeft(c, app, second, third), qt.Equals, 0)
		})
	}
}

// TestSchemaInspectRefusesADevServerThatIsNotCleanE2E is the refusal: a dev
// server holding a user database is refused in the binary's words, and the
// database is kept.
func TestSchemaInspectRefusesADevServerThatIsNotCleanE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "second"}, []string{"app"})

			out, err := runCompatVerb("schema", "inspect",
				"-u", "file://"+devServerSchemaFile(c, server.names["app"], server.names["second"]),
				"--dev-url", server.url)

			c.Assert(err, qt.ErrorMatches, `(?s).*connected database is not clean: found schema "`+server.names["app"]+`".*`,
				qt.Commentf("%s", out))
			c.Assert(server.databasesLeft(c, server.names["app"]), qt.Equals, 1)
		})
	}
}
