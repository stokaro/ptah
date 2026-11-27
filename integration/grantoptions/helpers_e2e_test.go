//go:build integration

package grantoptions_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestMain removes the binaries the command tests build.
func TestMain(m *testing.M) {
	clirun.Main(m)
}

// database holds one test's database and its permission recipient.
type database struct {
	url  string
	conn *dbschema.DatabaseConnection
	role string
}

// connect opens an address and closes its connection at the end of the test.
func connect(c *qt.C, address string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// uniqueName keeps server-wide fixture names distinct across test processes.
func uniqueName(c *qt.C) string {
	c.Helper()
	random := make([]byte, 6)
	_, err := rand.Read(random)
	c.Assert(err, qt.IsNil)
	return "ptah_option_" + hex.EncodeToString(random)
}

// scratch creates and removes a database on engine. The drop statement names
// that database as {name}, including any engine-specific connection cleanup.
func scratch(c *qt.C, engine dbtarget.Engine, drop string) database {
	c.Helper()
	address := dbtarget.URL(c, engine)
	admin := connect(c, address)
	name := uniqueName(c)
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), strings.ReplaceAll(drop, "{name}", name))
		c.Check(dropErr, qt.IsNil)
	})
	address, err = atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	return database{url: address, conn: connect(c, address)}
}

// postgresDatabase gives a test an empty PostgreSQL database.
func postgresDatabase(c *qt.C) database {
	c.Helper()
	return scratch(c, dbtarget.PostgreSQL, "DROP DATABASE {name} WITH (FORCE)")
}

// postgresRole is created before its database, so cleanup drops that database
// and its grants before removing the server-wide role.
func postgresRole(c *qt.C) string {
	c.Helper()
	admin := connect(c, dbtarget.URL(c, dbtarget.PostgreSQL))
	role := uniqueName(c)
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE "+role)
		c.Check(dropErr, qt.IsNil)
	})
	return role
}

// sqlServerDatabase holds an object and a user whose initial permissions are
// held. Every object and principal belongs to its throwaway database.
func sqlServerDatabase(c *qt.C, held string) database {
	c.Helper()
	db := scratch(c, dbtarget.SQLServer,
		"ALTER DATABASE {name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {name}")
	db.role = "reader"
	db.exec(c, []string{
		"CREATE TABLE dbo.t1 (id INT PRIMARY KEY, name NVARCHAR(50))",
		"INSERT INTO dbo.t1 VALUES (1, 'a')",
		"CREATE USER reader WITHOUT LOGIN",
		strings.ReplaceAll(held, "{role}", db.role),
	})
	return db
}

// exec runs the script statements in their rendered order.
func (d database) exec(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := d.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// selectOnT48 reads the base privilege and option independently.
func selectOnT48(c *qt.C, db database, role string) string {
	c.Helper()
	var held, option bool
	c.Assert(db.conn.QueryRowContext(c.Context(),
		"SELECT has_table_privilege($1, 't48', 'SELECT'), has_table_privilege($1, 't48', 'SELECT WITH GRANT OPTION')",
		role).Scan(&held, &option), qt.IsNil)
	return fmt.Sprintf("SELECT %t OPTION %t", held, option)
}

// grantRows reads each permission state from the catalog.
func grantRows(c *qt.C, conn *dbschema.DatabaseConnection, query string, args ...any) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), query, args...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var values []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// schemaFile writes one declaration in a fresh working directory.
func schemaFile(c *qt.C, text string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(text), 0o600), qt.IsNil)
	return path
}

// ptah runs the native command without sharing project configuration.
func ptah(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
}

// outcome supplies both streams when an assertion fails.
func outcome(result clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", result.ExitCode, result.Stdout, result.Stderr)
}
