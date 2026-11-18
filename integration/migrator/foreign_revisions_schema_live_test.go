//go:build integration

package migrator_test

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// foreignSchemaEngine is how one engine prepares a revisions schema for an
// account with DML grants alone. The statements are templates over {role},
// {app}, the database the run connects to, and {revs}, the revisions schema.
type foreignSchemaEngine struct {
	name string
	// admin reaches the server with an account that may create databases and
	// accounts.
	admin dbtarget.Engine
	// server runs on the admin connection: the database and the account.
	server []string
	// scratch runs in {app} as the admin before Ptah creates the tables.
	scratch []string
	// grants runs in {app} as the admin after the tables exist.
	grants []string
	// cleanup runs on the admin connection when the test ends.
	cleanup []string
	// widgets is the migration's own table, as the admin names it.
	widgets string
}

// The engines that check the privilege to create before the name, and so
// would refuse the guarded CREATE of tables a DBA created. Oracle has its own
// test, since its accounts are its schemas and its lock keeps a table.
var foreignSchemaEngines = []foreignSchemaEngine{
	postgresFamilyForeignSchema("postgresql", dbtarget.PostgreSQL),
	postgresFamilyForeignSchema("yugabytedb", dbtarget.YugabyteDB),
	{
		name:  "cockroachdb",
		admin: dbtarget.CockroachDB,
		server: []string{
			`CREATE DATABASE "{app}"`,
			`CREATE USER {role}`,
		},
		scratch: []string{`CREATE SCHEMA {revs}`},
		grants: []string{
			`GRANT CONNECT ON DATABASE "{app}" TO {role}`,
			`GRANT USAGE ON SCHEMA {revs} TO {role}`,
			`GRANT SELECT, INSERT, UPDATE, DELETE ON {revs}.schema_migrations, {revs}.schema_migrations_log TO {role}`,
			`GRANT CREATE, USAGE ON SCHEMA public TO {role}`,
		},
		cleanup: []string{`DROP DATABASE IF EXISTS "{app}" CASCADE`, `DROP USER IF EXISTS {role}`},
		widgets: "public.widgets",
	},
	mySQLFamilyForeignSchema("mysql", dbtarget.MySQLAdmin),
	mySQLFamilyForeignSchema("mariadb", dbtarget.MariaDBAdmin),
}

// postgresFamilyForeignSchema is a PostgreSQL-compatible server: the
// revisions schema sits in the run's database, and the role needs USAGE on it.
func postgresFamilyForeignSchema(name string, admin dbtarget.Engine) foreignSchemaEngine {
	return foreignSchemaEngine{
		name:  name,
		admin: admin,
		server: []string{
			`CREATE DATABASE "{app}"`,
			`CREATE ROLE {role} LOGIN PASSWORD 'ptah_password'`,
		},
		scratch: []string{`CREATE SCHEMA {revs}`},
		grants: []string{
			`GRANT USAGE ON SCHEMA {revs} TO {role}`,
			`GRANT SELECT, INSERT, UPDATE, DELETE ON {revs}.schema_migrations, {revs}.schema_migrations_log TO {role}`,
			`GRANT CREATE, USAGE ON SCHEMA public TO {role}`,
		},
		cleanup: []string{`DROP DATABASE IF EXISTS "{app}" WITH (FORCE)`, `DROP ROLE IF EXISTS {role}`},
		widgets: "public.widgets",
	}
}

// mySQLFamilyForeignSchema is a MySQL-compatible server, where a schema is a
// database: the revisions database sits beside the run's.
func mySQLFamilyForeignSchema(name string, admin dbtarget.Engine) foreignSchemaEngine {
	return foreignSchemaEngine{
		name:  name,
		admin: admin,
		server: []string{
			"CREATE DATABASE `{app}`",
			"CREATE DATABASE `{revs}`",
			"CREATE USER '{role}'@'%' IDENTIFIED BY 'ptah_password'",
			"GRANT ALL ON `{app}`.* TO '{role}'@'%'",
		},
		grants: []string{
			"GRANT SELECT, INSERT, UPDATE, DELETE ON `{revs}`.schema_migrations TO '{role}'@'%'",
			"GRANT SELECT, INSERT, UPDATE, DELETE ON `{revs}`.schema_migrations_log TO '{role}'@'%'",
		},
		cleanup: []string{"DROP DATABASE IF EXISTS `{app}`", "DROP DATABASE IF EXISTS `{revs}`", "DROP USER IF EXISTS '{role}'@'%'"},
		widgets: "widgets",
	}
}

// An account with DML grants alone records a full `migrate up` in a revisions
// schema a DBA prepared: the revision table and its log exist, and the
// account may read and write both and create nothing in their schema, nor any
// schema or database. Each engine here checks the privilege to create before
// the name, so the guarded CREATE of what is already there would be refused;
// Initialize and the log leave alone the tables the catalog shows, and the run
// creates only the migration's own table, where the account may create one.
func TestForeignRevisionsSchemaLive_HappyPath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	for _, engine := range foreignSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			stamp := time.Now().UnixNano() % 1_000_000_000_000
			names := strings.NewReplacer(
				"{role}", fmt.Sprintf("ptah_fs_%d", stamp),
				"{app}", fmt.Sprintf("ptah_fsa_%d", stamp),
				"{revs}", fmt.Sprintf("ptah_fsr_%d", stamp),
			)
			serverURL := dbtarget.URL(c, engine.admin)
			server := openForeignSchema(c, serverURL)
			c.Cleanup(func() {
				for _, statement := range engine.cleanup {
					_, err := server.ExecContext(context.Background(), names.Replace(statement))
					c.Check(err, qt.IsNil, qt.Commentf("statement: %s", names.Replace(statement)))
				}
			})
			execAll(c, server, names, engine.server)
			app := names.Replace("{app}")
			revs := names.Replace("{revs}")
			admin := openForeignSchema(c, foreignSchemaURL(c, serverURL, app, nil))
			execAll(c, admin, names, engine.scratch)
			// The DBA has Ptah create the tables once, then empties what that
			// run recorded and drops the table it created.
			prepare, err := migrator.NewFSMigrator(admin, oracleWidgets())
			c.Assert(err, qt.IsNil)
			c.Assert(prepare.WithMigrationsTable(revs, "").MigrateUp(context.Background()), qt.IsNil)
			execAll(c, admin, names, []string{
				"DROP TABLE " + engine.widgets,
				"DELETE FROM {revs}.schema_migrations",
				"DELETE FROM {revs}.schema_migrations_log",
			})
			execAll(c, admin, names, engine.grants)
			account := url.UserPassword(names.Replace("{role}"), "ptah_password")
			run, err := migrator.NewFSMigrator(openForeignSchema(c, foreignSchemaURL(c, serverURL, app, account)), oracleWidgets())
			c.Assert(err, qt.IsNil)

			err = run.WithMigrationsTable(revs, "").MigrateUp(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(readOne(c, admin, "SELECT COUNT(*) FROM "+engine.widgets), qt.Equals, "1")
			c.Assert(readOne(c, admin, "SELECT state FROM "+revs+".schema_migrations WHERE version = 1"), qt.Equals, "applied")
			c.Assert(readOne(c, admin, "SELECT COUNT(*) FROM "+revs+".schema_migrations_log WHERE version = 1"), qt.Not(qt.Equals), "0")
			c.Assert(schemaTables(c, admin, revs), qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log"})
		})
	}
}

func execAll(c *qt.C, conn *dbschema.DatabaseConnection, names *strings.Replacer, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(context.Background(), names.Replace(statement))
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", names.Replace(statement)))
	}
}

// foreignSchemaURL addresses database on the server serverURL names, as user,
// or as the registry's account when user is nil. A MySQL-family address in the
// driver's tcp() form is not a URL, so the driver rewrites it.
func foreignSchemaURL(c *qt.C, serverURL, database string, user *url.Userinfo) string {
	c.Helper()
	if scheme, dsn, found := strings.Cut(serverURL, "://"); found && strings.Contains(dsn, "@tcp(") {
		config, err := mysqldriver.ParseDSN(dsn)
		c.Assert(err, qt.IsNil)
		config.DBName = database
		if user != nil {
			config.User = user.Username()
			config.Passwd, _ = user.Password()
		}
		return scheme + "://" + config.FormatDSN()
	}
	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.User = cmp.Or(user, parsed.User)
	return parsed.String()
}

// openForeignSchema connects to address and closes the connection when the
// test ends.
func openForeignSchema(c *qt.C, address string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// readOne reads the one value query returns, as text.
func readOne(c *qt.C, conn *dbschema.DatabaseConnection, query string) string {
	c.Helper()
	var value string
	c.Assert(conn.QueryRowContext(context.Background(), query).Scan(&value), qt.IsNil, qt.Commentf("query: %s", query))
	return value
}

// schemaTables lists the base tables of schema, sorted.
func schemaTables(c *qt.C, conn *dbschema.DatabaseConnection, schema string) []string {
	c.Helper()
	rows, err := conn.QueryContext(context.Background(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '"+schema+"' AND table_type = 'BASE TABLE'")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rows.Close() })
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(names)
	return names
}
