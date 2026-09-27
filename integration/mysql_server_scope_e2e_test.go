//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
)

// mysqlServerEngines are the servers a URL naming no database is read from,
// each reached with an account that may create databases.
var mysqlServerEngines = []struct {
	name  string
	admin dbtarget.Engine
}{
	{name: "MySQL", admin: dbtarget.MySQLAdmin},
	{name: "MariaDB", admin: dbtarget.MariaDBAdmin},
}

// mysqlServer is two databases on one server, and the URL of the server
// itself. The first holds `t`; the second holds `u`, `x` with a foreign key to
// the first database's `t`, and `y` with a foreign key to `u` beside it.
type mysqlServer struct {
	url           string
	first, second string
}

func newMySQLServer(c *qt.C, engine dbtarget.Engine) mysqlServer {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	first, _ := scratch.builtFrom(c, `CREATE TABLE t (id int PRIMARY KEY);`)
	second, _ := scratch.database(c, "server")
	_, err := scratch.admin.ExecContext(c.Context(),
		"CREATE TABLE `"+second+"`.u (id int PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = scratch.admin.ExecContext(c.Context(),
		"CREATE TABLE `"+second+"`.x (id int PRIMARY KEY, t_id int, CONSTRAINT x_t FOREIGN KEY (t_id) REFERENCES `"+first+"`.t (id))")
	c.Assert(err, qt.IsNil)
	_, err = scratch.admin.ExecContext(c.Context(),
		"CREATE TABLE `"+second+"`.y (id int PRIMARY KEY, u_id int, CONSTRAINT y_u FOREIGN KEY (u_id) REFERENCES `"+second+"`.u (id))")
	c.Assert(err, qt.IsNil)
	server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
	c.Assert(err, qt.IsNil)
	return mysqlServer{url: server, first: first, second: second}
}

// TestSchemaInspectReadsAMySQLServerE2E reads a URL naming no database as the
// whole server, as the pinned community binary v1.3.0 reads it: measured on
// MySQL 8.4.11 and MariaDB 11.8.9, `schema inspect` describes every user
// database, each table under its database, and `--schema` picks databases
// (stokaro/ptah#3789). The catalog is shared with every other test, so the
// assertions name the databases this test made.
func TestSchemaInspectReadsAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			compat, err := runCompatVerb("schema", "inspect", "--url", server.url,
				"--schema", server.first, "--schema", server.second, "--format", "{{ sql . }}")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
			hcl, err := runCompatVerb("schema", "inspect", "--url", server.url)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hcl))
			native := runPtahNative(c, "schema", "inspect", "--db-url", server.url,
				"--schemas", server.first+","+server.second, "--format", "sql")

			for _, output := range []string{compat, native} {
				c.Assert(output, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+server.first+"`")
				c.Assert(output, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+server.second+"`")
				c.Assert(output, qt.Contains, "CREATE TABLE `"+server.first+"`.`t`")
				c.Assert(output, qt.Contains, "CREATE TABLE `"+server.second+"`.`x`")
				c.Assert(output, qt.Contains, "REFERENCES `"+server.first+"`.`t`(`id`)")
				c.Assert(output, qt.Contains, "REFERENCES `"+server.second+"`.`u`(`id`)")
			}
			c.Assert(hcl, qt.Contains, `schema "`+server.first+`" {`)
			c.Assert(hcl, qt.Contains, `schema "`+server.second+`" {`)
			c.Assert(hcl, qt.Contains, "schema = schema."+server.second)
			c.Assert(hcl, qt.Not(qt.Contains), `schema "mysql" {`)
			c.Assert(hcl, qt.Not(qt.Contains), `schema "sys" {`)
			c.Assert(hcl, qt.Not(qt.Contains), `schema "information_schema" {`)
		})
	}
}

// TestSchemaInspectNamesAReferenceInTheKeysOwnDatabaseAloneE2E writes a
// foreign key to a table of its own database as the table's name, the way the
// pinned community binary v1.3.0 writes it, and one to another database with
// that database, which the community binary drops.
func TestSchemaInspectNamesAReferenceInTheKeysOwnDatabaseAloneE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			document, err := runCompatVerb("schema", "inspect", "--url", server.url,
				"--schema", server.second, "--format", "{{ json . }}")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", document))
			c.Assert(document, qt.Contains, `"references":{"table":"u","columns":["id"]}`)
			c.Assert(document, qt.Contains, `"references":{"table":"`+server.first+`.t","columns":["id"]}`)
		})
	}
}

// mysqlServerRefusals are commands that read or change one database. Each
// refuses a URL naming no database before it connects, and says what to write
// instead: the pinned community binary v1.3.0 reads that URL as the whole
// server, and these commands do not yet (stokaro/ptah#3789).
var mysqlServerRefusals = []struct {
	name string
	args func(server, dir string) []string
}{
	{
		name: "migrate status",
		args: func(server, dir string) []string {
			return []string{"migrate", "status", "--url", server, "--dir", "file://" + dir}
		},
	},
	{
		name: "migrate apply",
		args: func(server, dir string) []string {
			return []string{"migrate", "apply", "--url", server, "--dir", "file://" + dir}
		},
	},
	{
		name: "migrate diff with the server as the dev database",
		args: func(server, dir string) []string {
			return []string{"migrate", "diff", "--dir", "file://" + dir, "--to", "file://" + dir, "--dev-url", server}
		},
	},
}

// TestCommandsForOneDatabaseRefuseAMySQLServerURLE2E is the control for the
// tests above: the verbs that do not read a whole server refuse the URL by
// name, and none of them reaches the server.
func TestCommandsForOneDatabaseRefuseAMySQLServerURLE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range mysqlServerRefusals {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				server := newMySQLServer(c, engine.admin)
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)

				out, err := runCompatVerb(test.args(server.url, dir)...)

				c.Assert(err, qt.ErrorMatches,
					`(?s).*the database URL names no database, and this command reads and changes one MySQL or MariaDB database, not a whole server.*`,
					qt.Commentf("%s", out))
			})
		}
	}
}
