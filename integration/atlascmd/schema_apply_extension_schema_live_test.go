//go:build integration

package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// TestCompatSchemaApplyCreatesExtensionInDeclaredSchemaPostgres proves the
// default compatibility profile retains the shared Pro-like placement
// capability. Strict CE policy remains covered by its separate refusal suite.
func TestCompatSchemaApplyCreatesExtensionInDeclaredSchemaPostgres(t *testing.T) {
	c := qt.New(t)
	dsn := livePostgresURLForRLSSpelling(t)
	targetURL, devURL := createRLSSpellingDatabases(t, dsn)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
schema "extensions" {}
extension "pgcrypto" {
  schema = schema.extensions
}
`), 0o600), qt.IsNil)

	first, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	second, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(first, qt.Contains, `CREATE EXTENSION "pgcrypto" WITH SCHEMA "extensions";`)
	c.Assert(second, qt.Contains, "Schema is synced, no changes to be made")
	c.Assert(compatExtensionInstallations(c, targetURL), qt.DeepEquals, []string{"extensions.pgcrypto"})
}

func compatExtensionInstallations(c *qt.C, dbURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), `
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
