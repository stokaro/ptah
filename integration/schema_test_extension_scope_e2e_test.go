//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestSchemaTestSchemaSelectionKeepsDatabaseWideExtensionPostgresE2E proves a
// schema-test allow-list does not treat an extension's installation schema as
// object ownership. The selected app table cannot be created unless citext is
// installed first in the otherwise-unselected extensions schema.
func TestSchemaTestSchemaSelectionKeepsDatabaseWideExtensionPostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	devURL := freshSchemaTestE2EDatabase(c, ctx, adminDB, adminURL, "extension_scope")
	schemaFile := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(schemaFile, []byte(`
schema "extensions" {}
extension "citext" {
  schema = schema.extensions
}

schema "app" {}
table "users" {
  schema = schema.app
  column "email" {
    type = sql("extensions.citext")
  }
}
`), 0o600), qt.IsNil)
	testDir := writeLiveTestCases(c, `cases:
  - name: selected schema keeps its database-wide extension
    steps:
      - name: extension placement survives selection
        assert:
          query: SELECT e.extname || ':' || n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'citext'
          scalar: citext:extensions
      - name: extension-backed table was materialized
        assert:
          query: SELECT email FROM app.users
          row_count: 0
`)

	output := runLivePtahCommand(c, ctx,
		"schema", "test",
		"--dir", testDir,
		"--root-dir", schemaFile,
		"--schema", "app",
		"--db-url", devURL,
	)

	c.Assert(output, qt.Contains, `PASS  case "selected schema keeps its database-wide extension"`)
}
