package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestSchemaDiffExcludeFiltersLocalSchemaFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`
CREATE TABLE diff_keep (
  id INTEGER PRIMARY KEY
);
CREATE TABLE diff_skip (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "sqlite://dev.db",
		"--exclude", "diff_skip",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
	c.Assert(out.String(), qt.Contains, "diff_keep")
	c.Assert(out.String(), qt.Not(qt.Contains), "diff_skip")
}

func TestSchemaApplyExcludeFiltersLocalSchemaFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE apply_keep (
  id INTEGER PRIMARY KEY
);
CREATE TABLE apply_skip (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://dev.db",
		"--dry-run",
		"--exclude", "apply_skip",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
	c.Assert(out.String(), qt.Contains, "apply_keep")
	c.Assert(out.String(), qt.Not(qt.Contains), "apply_skip")
}
