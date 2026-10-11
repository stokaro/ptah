//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// TestCompatSchemaApplyWritesTheSkippedExtensionLineMySQL pins `schema
// apply` on a target that skips a declared extension. The first apply
// creates the table and says the extension was skipped; the second has
// nothing to plan and still says so, ahead of the synced line, so "Schema is
// synced" never stands alone over a declaration the target left out.
func TestCompatSchemaApplyWritesTheSkippedExtensionLineMySQL(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.MySQLAdmin)
	targetURL := createSkipMySQLDatabase(c, adminURL, "ptah4140_skip_target")
	devURL := createSkipMySQLDatabase(c, adminURL, "ptah4140_skip_dev")
	database := skipMySQLDatabaseName(c, targetURL)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, fmt.Appendf(nil, `schema %[1]q {}
table "docs" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
extension "pg_trgm" {}
`, database), 0o600), qt.IsNil)
	skipped := `MYSQL: PostgreSQL extension "pg_trgm" is not supported by this target; skipped.`

	first, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	second, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))

	c.Assert(first, qt.Contains, "CREATE TABLE")
	c.Assert(first, qt.Contains, skipped)
	c.Assert(second, qt.Not(qt.Contains), "CREATE TABLE")
	c.Assert(second, qt.Contains, skipped)
	c.Assert(second, qt.Contains, "Schema is synced")
	c.Assert(strings.Index(second, skipped) < strings.Index(second, "Schema is synced"), qt.IsTrue, qt.Commentf("%s", second))
}

// createSkipMySQLDatabase creates a database for the test through the
// administrative connection and returns its URL, dropping it on cleanup.
func createSkipMySQLDatabase(c *qt.C, adminURL, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	quoted := sqlident.Quote(platform.MySQL, name)
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+quoted)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, cleanupErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoted)
		c.Check(cleanupErr, qt.IsNil)
	})
	scheme, dsn, found := strings.Cut(adminURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = name
	return scheme + "://" + config.FormatDSN()
}

// skipMySQLDatabaseName reads the database a MySQL URL names.
func skipMySQLDatabaseName(c *qt.C, dbURL string) string {
	c.Helper()
	_, dsn, found := strings.Cut(dbURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	return config.DBName
}
