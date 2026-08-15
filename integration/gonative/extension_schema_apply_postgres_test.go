//go:build integration

package gonative_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/root"
)

// TestNativeSchemaApplyCreatesExtensionInDeclaredSchemaPostgres crosses the
// native command, parser, shared planner, renderer, writer, and live catalog.
// The second apply pins convergence rather than only successful execution.
func TestNativeSchemaApplyCreatesExtensionInDeclaredSchemaPostgres(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	targetURL := newReadScopeDatabase(c, dsn, "native_ext_target", nil)
	devURL := newReadScopeDatabase(c, dsn, "native_ext_dev", nil)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
schema "extensions" {}
extension "pgcrypto" {
  schema = schema.extensions
}
`), 0o600), qt.IsNil)

	first := runNativeExtensionSchemaApply(c, targetURL, devURL, path)
	second := runNativeExtensionSchemaApply(c, targetURL, devURL, path)
	c.Assert(first, qt.Contains, `CREATE EXTENSION "pgcrypto" WITH SCHEMA "extensions";`)
	c.Assert(first, qt.Contains, "Schema apply completed successfully.")
	c.Assert(second, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(extensionInstallations(c, targetURL), qt.DeepEquals, []string{"extensions.pgcrypto"})
}

func runNativeExtensionSchemaApply(c *qt.C, targetURL, devURL, path string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--db-url", targetURL,
		"--schema-file", path,
		"--dev-url", devURL,
		"--auto-approve",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", output.String()))
	return output.String()
}

func extensionInstallations(c *qt.C, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(context.Background(), `
SELECT n.nspname || '.' || e.extname
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
 WHERE e.extname = 'pgcrypto'
 ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		found = append(found, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}
