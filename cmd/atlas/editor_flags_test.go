package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
)

func TestCompatCommand_MigrateNewEditOpensEditor(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	installAppendEditor(t, "-- authored in editor")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "new", "add_users", "--dir", "file://" + dir, "--edit"})

	err := cmd.Execute()

	// --edit forwards to the native `migrations create --edit`: the created
	// Atlas-format file opens in $EDITOR and atlas.sum is refreshed afterwards
	// so the directory still validates.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Generated empty migration file:")
	created, globErr := filepath.Glob(filepath.Join(dir, "*_add_users.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(created, qt.HasLen, 1)
	content, readErr := os.ReadFile(created[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- authored in editor")
	res, verifyErr := migratesum.VerifyDir(dir)
	c.Assert(verifyErr, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestCompatCommand_MigrateNewEditWithoutEditorFails(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "new", "add_users", "--dir", "file://" + t.TempDir(), "--edit"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `no editor configured: set \$EDITOR or \$VISUAL, or pass --editor`)
}

func TestCompatCommand_MigrateDiffEditOpensEditor(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)
	installAppendEditor(t, "-- reviewed in editor")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--edit",
		"add_email",
	})

	err := cmd.Execute()

	// --edit opens the generated migration in $EDITOR and atlas.sum is
	// refreshed afterwards, so the edited content still validates.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Created migration file:")
	files, globErr := filepath.Glob(filepath.Join(migrationsDir, "*_add_email.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	content, readErr := os.ReadFile(files[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- reviewed in editor")
	res, verifyErr := migratesum.VerifyDir(migrationsDir)
	c.Assert(verifyErr, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestCompatCommand_MigrateDiffEditRejectsDryRun(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--to", "file://schema.sql",
		"--dev-url", "sqlite://dev.db",
		"--edit", "--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`atlas migrate diff --edit cannot be combined with --dry-run: dry runs write no migration file to edit`)
}

func TestCompatCommand_SchemaApplyEditAppliesEditedSQL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	installAppendEditor(t, "CREATE TABLE audit_log (id INTEGER PRIMARY KEY);")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--edit",
		"--auto-approve",
	})

	err := cmd.Execute()

	// --edit round-trips the planned SQL through $EDITOR; the edited SQL is
	// displayed and applied, so the statement the editor appended lands in the
	// target database alongside the planned one.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Planned schema changes:")
	c.Assert(out.String(), qt.Contains, "audit_log")
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	assertEditorFlagsSQLiteTableExists(c, dbPath, "users")
	assertEditorFlagsSQLiteTableExists(c, dbPath, "audit_log")
}

func TestCompatCommand_SchemaApplyEditWithoutEditorFails(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "apply.db"),
		"--to", "file://" + schemaPath,
		"--edit",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `no editor configured: set \$EDITOR or \$VISUAL`)
}

func assertEditorFlagsSQLiteTableExists(c *qt.C, dbPath, table string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	for _, dbTable := range schema.Tables {
		if dbTable.Name == table {
			return
		}
	}
	c.Fatalf("table %s not found in %s", table, dbPath)
}
