package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

const scopeTestSchemaSQL = `
CREATE TABLE scope_users (
  id INTEGER PRIMARY KEY,
  email TEXT
);
CREATE TABLE scope_groups (
  id INTEGER PRIMARY KEY,
  owner_id INTEGER REFERENCES scope_users(id)
);
CREATE TABLE scope_archive (
  id INTEGER PRIMARY KEY
);
`

// writeScopeSchemaFiles writes an empty --from file and a --to file with the
// scope test schema and returns their paths together with a dev database
// path inside the same temp dir.
func writeScopeSchemaFiles(t *testing.T) (fromPath, toPath, devPath string) {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	fromPath = filepath.Join(dir, "from.sql")
	toPath = filepath.Join(dir, "to.sql")
	devPath = filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(fromPath, []byte(""), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	return fromPath, toPath, devPath
}

func TestSchemaDiffIncludeSelectsLocalSchemaFiles(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", "scope_users",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
	c.Assert(out.String(), qt.Contains, "scope_users")
	c.Assert(out.String(), qt.Not(qt.Contains), "scope_groups")
	c.Assert(out.String(), qt.Not(qt.Contains), "scope_archive")
}

func TestSchemaDiffIncludeUnionsRepeatedValues(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", "scope_groups",
		"--include", "scope_users",
	})

	err := cmd.Execute()

	// Both included tables survive, and the foreign key between them is an
	// internal dependency of the selection, so it is retained.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "scope_users")
	c.Assert(out.String(), qt.Contains, "scope_groups")
	c.Assert(out.String(), qt.Contains, "REFERENCES")
	c.Assert(out.String(), qt.Not(qt.Contains), "scope_archive")
}

func TestSchemaDiffIncludeEmptyMatchReportsSynced(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", "no_such_table",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schemas are synced, no changes to be made.")
}

func TestSchemaDiffIncludeCrossScopeDependencyFails(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", "scope_groups",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `table "scope_groups" depends on table "scope_users" via a foreign key, but "scope_users" is not selected`)
	c.Assert(out.String(), qt.Contains, "add the missing objects to the selection or exclude the dependent objects")
}

func TestSchemaDiffIncludeMalformedSelectorFailsBeforeDevDatabase(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
		"--include", "*[type=column]",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported Atlas include selector "\*\[type=column\]": column resources ride along with their parent and cannot be included on their own`)
	// Validation runs before any database work, so the dev database file was
	// never created.
	_, statErr := os.Stat(devPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaDiffSchemaScopeOnSQLite(t *testing.T) {
	c := qt.New(t)

	c.Run("main selects unqualified objects", func(c *qt.C) {
		fromPath, toPath, devPath := writeScopeSchemaFiles(t)
		cmd := atlas.NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{
			"schema", "diff",
			"--from", "file://" + fromPath,
			"--to", "file://" + toPath,
			"--dev-url", "sqlite://" + devPath,
			"--schema", "main",
		})

		err := cmd.Execute()

		c.Assert(err, qt.IsNil)
		c.Assert(out.String(), qt.Contains, "scope_users")
		c.Assert(out.String(), qt.Contains, "scope_archive")
	})

	c.Run("other schema selects nothing", func(c *qt.C) {
		fromPath, toPath, devPath := writeScopeSchemaFiles(t)
		cmd := atlas.NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{
			"schema", "diff",
			"--from", "file://" + fromPath,
			"--to", "file://" + toPath,
			"--dev-url", "sqlite://" + devPath,
			"--schema", "other",
		})

		err := cmd.Execute()

		c.Assert(err, qt.IsNil)
		c.Assert(out.String(), qt.Contains, "Schemas are synced, no changes to be made.")
	})
}

func TestSchemaApplyIncludeComposesWithExclude(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dry-run",
		"--include", "scope_users,scope_archive",
		"--exclude", "scope_archive",
	})

	err := cmd.Execute()

	// Positive selection defines the universe first; --exclude subtracts from
	// it afterward.
	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
	c.Assert(out.String(), qt.Contains, "scope_users")
	c.Assert(out.String(), qt.Not(qt.Contains), "scope_archive")
	c.Assert(out.String(), qt.Not(qt.Contains), "scope_groups")
}

func TestSchemaApplyIncludeEndToEndOnSQLite(t *testing.T) {
	c := qt.New(t)
	// The target already contains an out-of-scope table that the scoped apply
	// must leave untouched.
	dbPath := seedSQLiteDB(t, "CREATE TABLE scope_keepme (id INTEGER PRIMARY KEY)")
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--include", "scope_users",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteHasTable(t, dbPath, "scope_users"), qt.IsTrue)
	c.Assert(sqliteHasTable(t, dbPath, "scope_groups"), qt.IsFalse)
	c.Assert(sqliteHasTable(t, dbPath, "scope_archive"), qt.IsFalse)
	c.Assert(sqliteHasTable(t, dbPath, "scope_keepme"), qt.IsTrue)
}

func TestSchemaApplyIncludeValidationRunsBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		// The URL points at a closed port: reaching it would fail with a
		// connection error instead of the selector error asserted below.
		"--url", "postgres://127.0.0.1:1/unreachable",
		"--to", "file://" + schemaPath,
		"--include", "*[type=widget]",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported Atlas include resource type "widget" in selector "\*\[type=widget\]"`)
}

func TestSchemaApplyCrossScopeDependencyFails(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--include", "scope_groups",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `table "scope_groups" depends on table "scope_users" via a foreign key, but "scope_users" is not selected`)
}

func TestSchemaApplySchemaScopeEmptyMatchReportsSynced(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(scopeTestSchemaSQL), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--schema", "does_not_exist",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema is synced, no changes to be made.")
}
