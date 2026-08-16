//go:build integration

package sqlitecmd_test

// The virtual-table lifecycle against a real SQLite database, which is the
// acceptance criterion stokaro/ptah#1028 left uncovered: the two tests beside
// this one pin catalog safety and one dry-run plan, and nothing exercised
// introspection, replay onto a clean database, idempotence, or cleanup end to
// end.
//
// A unit test cannot answer these. Whether SQLite reports the module's shadow
// tables in the catalog, whether it accepts the declaration Ptah rendered back
// to it, and whether a second apply finds anything to do are all facts about
// the engine, not about the renderer.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/db"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

// fts5Schema declares one ordinary table beside the virtual one, so a test that
// loses the virtual table still has something to compare against and cannot
// pass by finding an empty schema on both sides. The tokenizer argument carries
// a quoted value and an embedded space, which is the shape SQLite stores as
// written.
const fts5Schema = `CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE VIRTUAL TABLE docs USING fts5(title, body, tokenize = 'porter unicode61');
`

// shadowTableNames are the storage tables the fts5 module creates for docs.
// They belong to the module, and a desired state that named them would plan
// statements SQLite refuses.
var shadowTableNames = []string{"docs_data", "docs_idx", "docs_content", "docs_docsize", "docs_config"}

func TestVirtualTableInspectionOmitsTheModulesShadowTables(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "docs.db")
	seedSQLite(c, dbPath, fts5Schema)

	out, err := runNativeInspect(dbPath, "--format", "sql")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `CREATE VIRTUAL TABLE "docs" USING fts5(title, body, tokenize = 'porter unicode61')`)
	c.Assert(out, qt.Contains, `CREATE TABLE "users"`)
	assertNamesAbsent(c, out, shadowTableNames)
}

func TestVirtualTableReplaysOntoACleanDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	origin := filepath.Join(dir, "origin.db")
	seedSQLite(c, origin, fts5Schema)
	exported, err := runNativeInspect(origin, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", exported))
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired, []byte(exported), 0o600), qt.IsNil)
	replayed := filepath.Join(dir, "replayed.db")

	applyOut, applyErr := runSchemaApply(replayed, desired)

	c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applyOut))
	// The replayed database is compared through the same reader that produced
	// the file, so a declaration that survived the round trip byte for byte is
	// what makes the two equal -- not a laxer comparison written for this test.
	reread, err := runNativeInspect(replayed, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", reread))
	c.Assert(reread, qt.Equals, exported)
}

func TestVirtualTableApplyIsIdempotent(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "docs.db")
	seedSQLite(c, dbPath, fts5Schema)
	exported, err := runNativeInspect(dbPath, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", exported))
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired, []byte(exported), 0o600), qt.IsNil)

	out, err := runSchemaDiff(dbPath, desired)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
}

func TestVirtualTableCleanupRemovesItAndItsShadowTables(t *testing.T) {
	c := qt.New(t)
	t.Setenv(sqlitevirtual.AllowDropEnvVar, "1")
	dbPath := filepath.Join(t.TempDir(), "docs.db")
	seedSQLite(c, dbPath, fts5Schema)

	out, err := runDBDropAll(dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// Read the emptied database rather than trusting the command's own report:
	// dropping a virtual table is what removes its storage, and a cleanup that
	// left the shadow tables behind would collide with the next CREATE.
	remaining, err := runNativeInspect(dbPath, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", remaining))
	assertNamesAbsent(c, remaining, append([]string{"docs", "users"}, shadowTableNames...))
}

func assertNamesAbsent(c *qt.C, out string, names []string) {
	c.Helper()
	for _, name := range names {
		c.Assert(out, qt.Not(qt.Contains), `"`+name+`"`, qt.Commentf("name %q in:\n%s", name, out))
	}
}

func runSchemaApply(dbPath, schemaFile string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"apply",
		"--db-url", "sqlite://" + dbPath,
		"--schema-file", schemaFile,
		"--auto-approve",
	})
	err := cmd.Execute()
	return out.String(), err
}

func runSchemaDiff(dbPath, schemaFile string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"diff",
		"--from", "sqlite://" + dbPath,
		"--to", "file://" + schemaFile,
	})
	err := cmd.Execute()
	return out.String(), err
}

func runDBDropAll(dbPath string) (string, error) {
	cmd := db.NewDBCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"drop-all",
		"--db-url", "sqlite://" + dbPath,
		"--auto-approve",
	})
	err := cmd.Execute()
	return strings.TrimSpace(out.String()), err
}
