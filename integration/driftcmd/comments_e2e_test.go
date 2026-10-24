//go:build integration

package driftcmd_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/safety"
)

// commentedSchema declares a table and a column with a comment each, in the
// schema it names.
func commentedSchema(schema string) string {
	return `schema "` + schema + `" {}

table "users" {
  schema  = schema.` + schema + `
  comment = "People who log in"
  column "id" {
    type = integer
  }
  column "email" {
    type    = text
    null    = true
    comment = "Where mail goes"
  }
  primary_key {
    columns = [column.id]
  }
}
`
}

// writeHCL writes document as the desired schema file.
func writeHCL(c *qt.C, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// TestSchemaDriftFindsSQLiteInSyncDespiteDeclaredCommentsE2E drives `ptah
// schema drift` against a SQLite database that holds the declared table. SQLite
// stores no comment on a table or a column, so its catalog reports none
// whatever the declaration says, and the database is in sync: a comment there
// is a difference on every run that no plan can close.
func TestSchemaDriftFindsSQLiteInSyncDespiteDeclaredCommentsE2E(t *testing.T) {
	c := qt.New(t)
	url := "sqlite://" + filepath.Join(c.TempDir(), "app.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `CREATE TABLE users (id integer PRIMARY KEY, email text)`)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	report := runDrift(c, "--db-url", url, "--schema-file", writeHCL(c, commentedSchema("main")), "--format", "json")

	c.Assert(report.Error, qt.Equals, "")
	c.Assert(report.Drift, qt.IsFalse)
	c.Assert(report.Findings, qt.IsNil)
}

// TestSchemaDriftReportsACommentPostgreSQLLacksE2E is the control: PostgreSQL
// stores both comments, so a database that lacks them differs, and the report
// names both changes.
func TestSchemaDriftReportsACommentPostgreSQLLacksE2E(t *testing.T) {
	c := qt.New(t)
	url := scratchDatabase(c, dbtarget.PostgreSQL, []string{`CREATE TABLE users (id integer PRIMARY KEY, email text)`})

	report := runDrift(c, "--db-url", url, "--schema-file", writeHCL(c, commentedSchema("public")), "--format", "json")

	c.Assert(report.Error, qt.Equals, "")
	c.Assert(report.Drift, qt.IsTrue)
	c.Assert(report.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "columns_modified", Count: 1, Severity: safety.Warning},
		{Category: "comments_changed", Count: 1, Severity: safety.Safe},
	})
}
