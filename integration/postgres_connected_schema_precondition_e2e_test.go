//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A plan creates the schema an added object names before the object, except
// the schema the connection resolved its unqualified names to, which exists.
// An Atlas HCL document names that schema on every table, because a table
// block cannot name a schema the document does not declare, so these tests
// read an HCL document. The database starts empty.

// connectedSchemaDocument declares public and app, with one table in each.
// The database holds public and not app.
const connectedSchemaDocument = `schema "public" {}
schema "app" {}

table "users" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}

table "accounts" {
  schema = schema.app
  column "id" {
    type = integer
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}
`

// connectedSchemaPublicOnly declares public and one table in it.
const connectedSchemaPublicOnly = `schema "public" {}

table "users" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}
`

const (
	connectedSchemaCreatesPublic = `CREATE SCHEMA IF NOT EXISTS "public"`
	connectedSchemaCreatesApp    = `CREATE SCHEMA IF NOT EXISTS "app"`
	connectedSchemaUsers         = `CREATE TABLE "public"."users"`
	connectedSchemaAccounts      = `CREATE TABLE "app"."accounts"`
)

// TestPlansDoNotCreateTheConnectedSchemaE2E runs each verb that prints a plan
// over the two-schema document. Each creates app, which the database does not
// hold, and does not create public, which the connection resolved to.
func TestPlansDoNotCreateTheConnectedSchemaE2E(t *testing.T) {
	tests := []struct {
		name string
		args func(database, schema string) []string
	}{
		{
			// Not --dry-run, which prints the plan file as JSON and escapes
			// every quote; the plan is saved into the run's directory.
			name: "schema plan",
			args: func(database, schema string) []string {
				return []string{"schema", "plan", "--db-url", database, "--schema-file", schema, "--save"}
			},
		},
		{
			name: "schema apply --dry-run",
			args: func(database, schema string) []string {
				return []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run"}
			},
		},
		{
			name: "schema compare",
			args: func(database, schema string) []string {
				return []string{"schema", "compare", "--db-url", database, "--schema-file", schema}
			},
		},
		{
			name: "migrations plan",
			args: func(database, schema string) []string {
				return []string{"migrations", "plan", "--db-url", database, "--schema-file", schema}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "connected")

			run := runPtahIn(c, test.args(database, writeConnectedSchemaDocument(c, connectedSchemaDocument))...)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, connectedSchemaUsers)
			c.Assert(run.Stdout, qt.Contains, connectedSchemaAccounts)
			c.Assert(run.Stdout, qt.Contains, connectedSchemaCreatesApp)
			c.Assert(run.Stdout, qt.Not(qt.Contains), connectedSchemaCreatesPublic)
		})
	}
}

// TestMigrationsGenerateDoesNotCreateTheConnectedSchemaE2E is the same plan
// written into a migration file.
func TestMigrationsGenerateDoesNotCreateTheConnectedSchemaE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "connected")
	migrations := c.TempDir()

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database,
		"--schema-file", writeConnectedSchemaDocument(c, connectedSchemaDocument), "--migrations-dir", migrations)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := readOneUpMigration(c, migrations)
	c.Assert(up, qt.Contains, connectedSchemaUsers)
	c.Assert(up, qt.Contains, connectedSchemaCreatesApp)
	c.Assert(up, qt.Not(qt.Contains), connectedSchemaCreatesPublic)
}

// TestApplyCreatesTheSchemaTheDatabaseLacksE2E applies the two-schema document
// and reads both tables back: the schema the plan does create is still
// created, ahead of the table that needs it.
func TestApplyCreatesTheSchemaTheDatabaseLacksE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "connected")

	applied := runPtahIn(c, "schema", "apply", "--db-url", database,
		"--schema-file", writeConnectedSchemaDocument(c, connectedSchemaDocument), "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(connectedSchemaTables(c, database), qt.Equals, "app.accounts, public.users")
}

// TestApplyNeedsNoDatabasePrivilegeForTheConnectedSchemaE2E applies the
// public-only document as a role that may create tables in public and may not
// create schemas in the database. PostgreSQL checks CREATE on the database
// before it looks for the schema, so CREATE SCHEMA IF NOT EXISTS "public" is
// refused to that role with SQLSTATE 42501 even though public exists; the
// plan carries none, and the table is created.
func TestApplyNeedsNoDatabasePrivilegeForTheConnectedSchemaE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin := openReadScopeDatabase(c, adminURL)
	role := fmt.Sprintf("ptah_connected_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(role)+" LOGIN PASSWORD 'connected-password'")
	c.Assert(err, qt.IsNil)
	// Registered before the database, so it runs after the database is
	// dropped: the role owns the table and holds a grant on public there.
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil)
	})
	database := newEmptyReadScopeDatabase(c, "connected")
	execPostgresFamilySQL(c, c.Context(), database, "GRANT USAGE, CREATE ON SCHEMA public TO "+quoteE2EIdent(role))
	asRole := connectedSchemaURLAs(c, database, role, "connected-password")

	applied := runPtahIn(c, "schema", "apply", "--db-url", asRole,
		"--schema-file", writeConnectedSchemaDocument(c, connectedSchemaPublicOnly), "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Not(qt.Contains), connectedSchemaCreatesPublic)
	c.Assert(connectedSchemaTables(c, database), qt.Equals, "public.users")
}

// writeConnectedSchemaDocument writes document to an HCL file and returns its
// path.
func writeConnectedSchemaDocument(c *qt.C, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.pg.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// connectedSchemaURLAs is database reached as role.
func connectedSchemaURLAs(c *qt.C, database, role, password string) string {
	c.Helper()
	parsed, err := url.Parse(database)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

// connectedSchemaTables lists the tables of public and app as schema.table,
// in name order.
func connectedSchemaTables(c *qt.C, database string) string {
	c.Helper()
	var tables string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(table_schema || '.' || table_name, ', ' ORDER BY table_schema, table_name), '')
		FROM information_schema.tables
		WHERE table_schema IN ('public', 'app') AND table_type = 'BASE TABLE'`).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}
