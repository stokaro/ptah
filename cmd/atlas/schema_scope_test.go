package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
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

// TestSchemaDiffIncludeEmptyMatchRefuses pins that a selector matching neither
// side cannot report a synced schema to a CI check.
func TestSchemaDiffIncludeEmptyMatchRefuses(t *testing.T) {
	c := qt.New(t)
	fromPath, toPath, devPath := writeScopeSchemaFiles(t)

	stdout, stderr, err := runCompat("schema", "diff",
		"--from", "file://"+fromPath,
		"--to", "file://"+toPath,
		"--dev-url", "sqlite://"+devPath,
		"--include", "no_such_table",
	)

	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "no_such_table"`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `Error: the --include selection matched no objects: "no_such_table"`+"\n")
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

// TestSchemaDiffIncludeSelectsQuotedDottedIdentifier guards the qualified
// spelling of an identifier that itself contains a dot. Ptah quotes such a
// part when it builds the schema-qualified candidate, so the selector that
// matches `main."dotted.table"` carries two dot characters but only one
// separator. A depth check that counted characters made this selector — and
// therefore the qualified spelling of every dotted identifier — inexpressible.
func TestSchemaDiffIncludeSelectsQuotedDottedIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "schema qualified", pattern: `main."dotted.table"`},
		{name: "wildcard schema", pattern: `*."dotted.table"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			dir := t.TempDir()
			fromPath := filepath.Join(dir, "from.sql")
			toPath := filepath.Join(dir, "to.sql")
			devPath := filepath.Join(dir, "dev.db")
			c.Assert(os.WriteFile(fromPath, []byte(""), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(toPath, []byte(
				"CREATE TABLE \"dotted.table\" (id INTEGER PRIMARY KEY);\nCREATE TABLE scope_archive (id INTEGER PRIMARY KEY);\n",
			), 0o600), qt.IsNil)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"schema", "diff",
				"--from", "file://" + fromPath,
				"--to", "file://" + toPath,
				"--dev-url", "sqlite://" + devPath,
				"--include", test.pattern,
			})

			err := cmd.Execute()

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Contains, `"dotted.table"`)
			c.Assert(out.String(), qt.Not(qt.Contains), "scope_archive")
		})
	}
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
	t.Run("main selects unqualified objects", func(t2 *testing.T) {
		c := qt.New(t2)
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

	t.Run("other schema selects nothing", func(t2 *testing.T) {
		c := qt.New(t2)
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
	allowSchemaApplyWithoutDevURL(t)
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
	allowSchemaApplyWithoutDevURL(t)
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
	allowSchemaApplyWithoutDevURL(t)
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
	allowSchemaApplyWithoutDevURL(t)
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
	c.Assert(out.String(), qt.Contains, "Schema is synced, no changes to be made")
}
