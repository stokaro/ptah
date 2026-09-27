//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
)

// serverRealm is the desired state of three databases on one server: the
// first database of [mysqlServer] with a character set of its own, a third
// database the server does not hold, with a table referencing the first, and
// not the second, which the server holds. Every run names the three with
// --schema, because the server is shared with every other test and a run
// over all of it would drop their databases.
func serverRealm(server mysqlServer, third string) string {
	return fmt.Sprintf(`schema %[1]q {
  charset = "latin1"
  collate = "latin1_swedish_ci"
}
schema %[2]q {
  charset = "utf8mb4"
  collate = "utf8mb4_bin"
}
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
  column "t_id" {
    null = true
    type = int
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "w_t" {
    columns     = [column.t_id]
    ref_columns = [table.t.column.id]
  }
}
`, server.first, third)
}

// schemaFlags repeats --schema for each database.
func schemaFlags(databases []string) []string {
	flags := make([]string, 0, 2*len(databases))
	for _, database := range databases {
		flags = append(flags, "--schema", database)
	}
	return flags
}

// serverRealmFixture is a whole server, the realm file for it, and the
// databases a run over it covers.
type serverRealmFixture struct {
	server    mysqlServer
	scratch   mysqlScratch
	third     string
	realm     string
	databases []string
}

func newServerRealmFixture(c *qt.C, engine dbtarget.Engine) serverRealmFixture {
	c.Helper()
	server := newMySQLServer(c, engine)
	scratch := newMySQLFamilyScratch(c, engine)
	third := fmt.Sprintf("ptah_server_new_%d", time.Now().UnixNano())
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, third) })
	realm := filepath.Join(c.TempDir(), "realm.hcl")
	c.Assert(os.WriteFile(realm, []byte(serverRealm(server, third)), 0o600), qt.IsNil)
	return serverRealmFixture{
		server: server, scratch: scratch, third: third, realm: realm,
		databases: []string{server.first, server.second, third},
	}
}

// TestCompatSchemaApplyPlansTheDatabasesOfAMySQLServerE2E applies a desired
// state to a whole server, as the pinned community binary v1.3.0 applies it:
// measured on MySQL 8.4.11 and MariaDB 11.8.9, it changes a database's
// collation, drops a database the file leaves out, and creates one it adds,
// with the table in it (stokaro/ptah#3789). Planned again, nothing is left,
// where the community binary plans the table's collation and the foreign
// key's index a second time. The databases are read back from the catalog.
func TestCompatSchemaApplyPlansTheDatabasesOfAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newServerRealmFixture(c, engine.admin)
			args := []string{"schema", "apply", "--url", fixture.server.url, "--to", "file://" + fixture.realm}

			applied, err := runCompatVerb(slices.Concat(args, []string{"--auto-approve"}, schemaFlags(fixture.databases))...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			again, err := runCompatVerb(slices.Concat(args, []string{"--dry-run"}, schemaFlags(fixture.databases))...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))

			c.Assert(applied, qt.Contains, "DROP DATABASE `"+fixture.server.second+"`")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.server.first), qt.Equals, "latin1_swedish_ci")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.third), qt.Equals, "utf8mb4_bin")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.server.second), qt.Equals, "")
			c.Assert(fixture.scratch.tablesOf(c, fixture.third), qt.Equals, "w")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyPlansTheDatabasesOfAMySQLServerE2E is the same apply through
// the native command, which reaches the same planning.
func TestSchemaApplyPlansTheDatabasesOfAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newServerRealmFixture(c, engine.admin)
			databases := strings.Join(fixture.databases, ",")

			applied := runPtahNative(c, "schema", "apply", "--db-url", fixture.server.url,
				"--schema-file", fixture.realm, "--schemas", databases, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", fixture.server.url,
				"--schema-file", fixture.realm, "--schemas", databases, "--dry-run")

			c.Assert(applied, qt.Contains, "DROP DATABASE `"+fixture.server.second+"`")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.server.first), qt.Equals, "latin1_swedish_ci")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.third), qt.Equals, "utf8mb4_bin")
			c.Assert(databaseCollation(c, fixture.scratch, fixture.server.second), qt.Equals, "")
			c.Assert(fixture.scratch.tablesOf(c, fixture.third), qt.Equals, "w")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// databaseCollation answers the default collation of a database, read from
// the catalog, and "" for a database the server does not hold.
func databaseCollation(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var collation string
	err := scratch.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(MAX(DEFAULT_COLLATION_NAME), '')
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME = ?`, database).Scan(&collation)
	c.Assert(err, qt.IsNil)
	return collation
}

// tablesOf answers the tables of a database, in name order.
func (s mysqlScratch) tablesOf(c *qt.C, database string) string {
	c.Helper()
	var tables string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME), '')
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`, database).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

// TestSchemaDiffFindsAMySQLServerSyncedWithItselfE2E compares a whole server
// with itself, databases included: nothing to plan.
func TestSchemaDiffFindsAMySQLServerSyncedWithItselfE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			compat, err := runCompatVerb(slices.Concat([]string{"schema", "diff", "--from", server.url, "--to", server.url},
				schemaFlags([]string{server.first, server.second}))...)
			native := runPtahNative(c, "schema", "diff", "--from", server.url, "--to", server.url,
				"--schemas", server.first+","+server.second)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
			c.Assert(compat, qt.Contains, "Schemas are synced")
			c.Assert(native, qt.Contains, "synced")
		})
	}
}

// TestSchemaApplyRefusesADevDatabaseBesideAMySQLServerE2E refuses a dev
// database beside a whole server before the dev database is contacted: a dev
// server that replays a desired state as a server is not taken yet
// (stokaro/ptah#3789). The dev URL names the server's own `mysql` database,
// which the dev clean check would refuse with another message, so the
// refusal is the first thing either command says about it. Both runs are dry
// runs, so a refusal that stopped working changes nothing.
func TestSchemaApplyRefusesADevDatabaseBesideAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)
			realm := filepath.Join(c.TempDir(), "realm.hcl")
			c.Assert(os.WriteFile(realm, []byte(`schema "`+server.first+`" {}`+"\n"), 0o600), qt.IsNil)
			dev := asMySQLURL(dbtarget.URL(c, engine.admin))

			compat, compatErr := runCompatVerb("schema", "apply", "--url", server.url, "--to", "file://"+realm,
				"--dev-url", dev, "--schema", server.first, "--dry-run")
			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", server.url,
				"--schema-file", realm, "--dev-url", dev, "--schemas", server.first, "--dry-run")

			c.Assert(compatErr, qt.ErrorMatches, `(?s).*a dev database for a whole MySQL or MariaDB server is not supported yet.*`,
				qt.Commentf("%s", compat))
			c.Assert(nativeErr, qt.ErrorMatches, `(?s).*a dev database for a whole MySQL or MariaDB server is not supported yet.*`,
				qt.Commentf("%s", native))
			c.Assert(databaseCollation(c, newMySQLFamilyScratch(c, engine.admin), server.second), qt.Not(qt.Equals), "")
		})
	}
}

// TestSchemaCleanPlansEveryDatabaseOfAMySQLServerE2E plans the cleanup of a
// whole server without running it: every user database, the foreign key one
// keeps into another first. The native command reports the same scope. The
// runs are dry runs because the server is shared with every other test.
func TestSchemaCleanPlansEveryDatabaseOfAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			out, err := runCompatVerb("schema", "clean", "--url", server.url, "--dry-run")
			native := runPtahNative(c, "db", "drop-all", "--db-url", server.url, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "ALTER TABLE `"+server.second+"`.`x` DROP FOREIGN KEY `x_t`")
			c.Assert(out, qt.Contains, "DROP DATABASE `"+server.first+"`")
			c.Assert(out, qt.Contains, "DROP DATABASE `"+server.second+"`")
			c.Assert(out, qt.Not(qt.Contains), "DROP DATABASE `mysql`")
			c.Assert(native, qt.Contains, "The URL names no database: every user database on the server is dropped.")
			c.Assert(databaseCollation(c, newMySQLFamilyScratch(c, engine.admin), server.first), qt.Not(qt.Equals), "")
		})
	}
}

// TestSchemaCleanRefusesSelectorsOnAMySQLServerE2E refuses --exclude on a
// whole server before anything is planned: the command drops every user
// database, and a selector it ignored would drop what it named. The run is a
// dry run, so a refusal that stopped working drops nothing on a server shared
// with every other test.
func TestSchemaCleanRefusesSelectorsOnAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			out, err := runCompatVerb("schema", "clean", "--url", server.url, "--exclude", server.first, "--dry-run")

			c.Assert(err, qt.ErrorMatches, `(?s).*--include and --exclude are not supported when the URL names no database.*`,
				qt.Commentf("%s", out))
			c.Assert(databaseCollation(c, newMySQLFamilyScratch(c, engine.admin), server.second), qt.Not(qt.Equals), "")
		})
	}
}

// mysqlServerBesideOneDatabase are the commands that compare two database
// reads, each given a whole server on one side and one database on the other.
// The pinned community binary v1.3.0 refuses each pair with the message in
// the row, measured on MySQL 8.4.11: read as a server, the one database would
// be all the desired state declares, and every other database would be
// dropped.
var mysqlServerBesideOneDatabase = []struct {
	name    string
	args    func(server, database string) []string
	wantErr string
}{
	{
		name: "schema diff from the server",
		args: func(server, database string) []string {
			return []string{"schema", "diff", "--from", server, "--to", database}
		},
		wantErr: `cannot diff a schema %q with a database connection`,
	},
	{
		name: "schema diff to the server",
		args: func(server, database string) []string {
			return []string{"schema", "diff", "--from", database, "--to", server}
		},
		wantErr: `cannot diff a database connection with a schema %q`,
	},
	{
		name: "schema apply to the server",
		args: func(server, database string) []string {
			return []string{"schema", "apply", "--url", server, "--to", database, "--dry-run"}
		},
		wantErr: `cannot diff a schema %q with a database connection`,
	},
	{
		name: "schema apply of the server",
		args: func(server, database string) []string {
			return []string{"schema", "apply", "--url", database, "--to", server, "--dry-run"}
		},
		wantErr: `cannot diff a database connection with a schema %q`,
	},
}

// TestCompatSchemaCommandsRefuseAMySQLServerBesideOneDatabaseE2E refuses a
// whole server compared with one database, in either order, and nothing is
// dropped. Each apply is a dry run, so a refusal that stopped working drops
// nothing on a server shared with every other test.
func TestCompatSchemaCommandsRefuseAMySQLServerBesideOneDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range mysqlServerBesideOneDatabase {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				server := newMySQLServer(c, engine.admin)
				database, err := atlasurl.WithDatabaseName(server.url, server.first)
				c.Assert(err, qt.IsNil)

				out, err := runCompatVerb(test.args(server.url, database)...)

				c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(fmt.Sprintf(test.wantErr, server.first))+`.*`,
					qt.Commentf("%s", out))
				c.Assert(databaseCollation(c, newMySQLFamilyScratch(c, engine.admin), server.second), qt.Not(qt.Equals), "")
			})
		}
	}
}

// TestCompatSchemaApplyTakesAMySQLServerAsTheDesiredStateE2E applies a whole
// server to itself, as the pinned community binary v1.3.0 applies one server
// to another: the desired state is every database the server holds, so
// nothing is planned.
func TestCompatSchemaApplyTakesAMySQLServerAsTheDesiredStateE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServer(c, engine.admin)

			out, err := runCompatVerb("schema", "apply", "--url", server.url, "--to", server.url,
				"--schema", server.first, "--schema", server.second, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}

// mysqlAccountServer is a whole server as one account sees it: the account
// holds privileges on the two databases of a [mysqlServer] and on nothing
// else, so a read of the server through url describes those two databases
// alone. A command that cleans the whole server through it drops them and
// nothing else the shared server holds.
type mysqlAccountServer struct {
	mysqlServer
	scratch mysqlScratch
}

func newMySQLAccountServer(c *qt.C, engine dbtarget.Engine) mysqlAccountServer {
	c.Helper()
	server := newMySQLServer(c, engine)
	family := newMySQLFamilyServer(c, engine)
	account := fmt.Sprintf("ptah_srv_%d", time.Now().UnixNano()%1_000_000_000)
	password := account + "_pw"
	createMySQLUser(c, c.Context(), family.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, account) })
	for _, database := range []string{server.first, server.second} {
		_, err := family.admin.ExecContext(c.Context(),
			fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%%'", database, account))
		c.Assert(err, qt.IsNil)
	}
	config := family.config.Clone()
	config.User, config.Passwd, config.DBName = account, password, ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var visible string
	err = db.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(SCHEMA_NAME ORDER BY SCHEMA_NAME), '')
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`).Scan(&visible)
	c.Assert(err, qt.IsNil)
	// The precondition every destructive run below stands on: the account sees
	// the two databases and nothing another test keeps.
	c.Assert(visible, qt.Equals, strings.Join(slices.Sorted(slices.Values([]string{server.first, server.second})), ","))
	server.url = (&url.URL{Scheme: "mysql", User: url.UserPassword(account, password), Host: config.Addr, Path: "/"}).String()
	return mysqlAccountServer{mysqlServer: server, scratch: newMySQLFamilyScratch(c, engine)}
}

// TestSchemaCleanDropsEveryDatabaseOfAMySQLServerE2E cleans a whole server
// for real, through an account that sees two databases, one holding a foreign
// key into the other. Both are dropped: the key first, which the server
// requires before the database it references goes. The pinned community
// binary v1.3.0 drops the databases in name order and stops with error 3730
// when the referenced one comes first, measured on MySQL 8.4.11.
func TestSchemaCleanDropsEveryDatabaseOfAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLAccountServer(c, engine.admin)

			out, err := runCompatVerb("schema", "clean", "--url", server.url, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(databaseCollation(c, server.scratch, server.first), qt.Equals, "")
			c.Assert(databaseCollation(c, server.scratch, server.second), qt.Equals, "")
		})
	}
}

// TestDBDropAllDropsEveryDatabaseOfAMySQLServerE2E is the same cleanup through
// the native command.
func TestDBDropAllDropsEveryDatabaseOfAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLAccountServer(c, engine.admin)

			out := runPtahNative(c, "db", "drop-all", "--db-url", server.url, "--auto-approve")

			c.Assert(out, qt.Contains, "every user database on the server is dropped")
			c.Assert(databaseCollation(c, server.scratch, server.first), qt.Equals, "")
			c.Assert(databaseCollation(c, server.scratch, server.second), qt.Equals, "")
		})
	}
}
