//go:build integration

package migrator_test

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A role with DML grants alone records a full `migrate up` in a revisions
// schema a DBA prepared: the revision table and its log exist, the role may
// use the schema and read and write both tables, and it may create nothing
// there and no schema in the database. PostgreSQL checks the privilege to
// create before the name, so CREATE SCHEMA IF NOT EXISTS and CREATE TABLE IF
// NOT EXISTS of what is already there answer "permission denied"; Initialize
// and the log leave alone the tables the catalog shows, and the run creates
// only the migration's own table, in a schema the role may create in.
func TestPostgresForeignRevisionsSchemaLive_HappyPath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	c := qt.New(t)
	stamp := time.Now().UnixNano()
	role := fmt.Sprintf("ptah_dml_%d", stamp)
	database := fmt.Sprintf("ptah_dml_%d", stamp)
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	server := openPostgres(c, serverURL)
	_, err := server.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`)
	c.Assert(err, qt.IsNil)
	_, err = server.ExecContext(context.Background(), `CREATE ROLE `+role+` LOGIN PASSWORD 'ptah_password'`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := server.ExecContext(ctx, `DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
		_, dropErr = server.ExecContext(ctx, `DROP ROLE IF EXISTS `+role)
		c.Check(dropErr, qt.IsNil)
	})
	adminURL := postgresDatabaseURL(c, serverURL, database, nil)
	admin := openPostgres(c, adminURL)
	exec := func(statement string) {
		c.Helper()
		_, execErr := admin.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	// The DBA has Ptah create the tables once, then empties what that run
	// recorded and drops the table it created.
	exec(`CREATE SCHEMA revs`)
	prepare, err := migrator.NewFSMigrator(admin, oracleWidgets())
	c.Assert(err, qt.IsNil)
	c.Assert(prepare.WithMigrationsTable("revs", "").MigrateUp(context.Background()), qt.IsNil)
	exec(`DROP TABLE public.widgets`)
	exec(`DELETE FROM revs.schema_migrations`)
	exec(`DELETE FROM revs.schema_migrations_log`)
	exec(`GRANT USAGE ON SCHEMA revs TO ` + role)
	exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON revs.schema_migrations, revs.schema_migrations_log TO ` + role)
	exec(`GRANT CREATE, USAGE ON SCHEMA public TO ` + role)
	run, err := migrator.NewFSMigrator(
		openPostgres(c, postgresDatabaseURL(c, serverURL, database, url.UserPassword(role, "ptah_password"))),
		oracleWidgets(),
	)
	c.Assert(err, qt.IsNil)

	err = run.WithMigrationsTable("revs", "").MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	read := func(query string) string {
		c.Helper()
		var value string
		c.Assert(admin.QueryRowContext(context.Background(), query).Scan(&value), qt.IsNil, qt.Commentf("query: %s", query))
		return value
	}
	c.Assert(read(`SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = 'widgets'`), qt.Equals, role)
	c.Assert(read(`SELECT COUNT(*) FROM public.widgets`), qt.Equals, "1")
	c.Assert(read(`SELECT state FROM revs.schema_migrations WHERE version = 1`), qt.Equals, "applied")
	c.Assert(read(`SELECT COUNT(*) FROM revs.schema_migrations_log WHERE version = 1`), qt.Not(qt.Equals), "0")
	c.Assert(postgresTables(c, admin, "revs"), qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log"})
}

// postgresDatabaseURL addresses database on the server serverURL names, as
// user, or as the registry's account when user is nil.
func postgresDatabaseURL(c *qt.C, serverURL, database string, user *url.Userinfo) string {
	c.Helper()
	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.User = cmp.Or(user, parsed.User)
	return parsed.String()
}

// openPostgres connects to address and closes the connection when the test
// ends.
func openPostgres(c *qt.C, address string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// postgresTables lists the tables of schema, sorted.
func postgresTables(c *qt.C, conn *dbschema.DatabaseConnection, schema string) []string {
	c.Helper()
	rows, err := conn.QueryContext(context.Background(), `SELECT tablename FROM pg_tables WHERE schemaname = $1`, schema)
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
