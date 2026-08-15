//go:build integration

package sqlitecmd_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

func TestSchemaInspectRefusesExplicitIndexOnVirtualTableShadowStorage(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "shadow-index.db")
	seedSQLite(c, dbPath, `
CREATE VIRTUAL TABLE docs USING fts5(title, body);
CREATE INDEX my_idx ON docs_content(c0);
`)

	out, err := runNativeInspect(dbPath, "--format", "sql")

	c.Assert(err, qt.ErrorMatches,
		`read database schema: sqlite: index "my_idx" targets virtual-table shadow table "docs_content"; .*`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "CREATE INDEX")
}

func TestSchemaApplyDryRunPreservesExactVirtualTableRemovalIdentity(t *testing.T) {
	c := qt.New(t)
	t.Setenv(sqlitevirtual.AllowDropEnvVar, "1")
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "virtual.db")
	seedSQLite(c, dbPath, `
CREATE TABLE docs (id INTEGER PRIMARY KEY);
CREATE VIRTUAL TABLE " docs " USING fts5(body);
`)
	desiredPath := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desiredPath, []byte("CREATE TABLE docs (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"apply",
		"--db-url", "sqlite://" + dbPath,
		"--schema-file", desiredPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `DROP TABLE IF EXISTS " docs ";`)
	c.Assert(out.String(), qt.Not(qt.Contains), `DROP TABLE IF EXISTS "docs";`)
}
