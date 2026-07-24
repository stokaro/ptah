package migrationsimport_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrationsimport"
)

func execute(args ...string) (stdout string, err error) {
	cmd := migrationsimport.NewMigrationsImportCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), err
}

func writeGolangMigrateSource(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_init.up.sql":   "CREATE TABLE t (id int);\n",
		"1_init.down.sql": "DROP TABLE t;\n",
		"2_add_c.up.sql":  "ALTER TABLE t ADD c text;\n",
		"README.md":       "# migrations\n",
	}
	for name, content := range files {
		qt.Assert(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

func TestImportCommand_WritesPtahFiles(t *testing.T) {
	c := qt.New(t)
	src := writeGolangMigrateSource(t)
	out := t.TempDir()

	stdout, err := execute("--source-dir", src, "--migrations-dir", out)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Wrote 4 migration file(s)")
	c.Assert(stdout, qt.Contains, "0000000001_init.up.sql")

	_, err = os.Stat(filepath.Join(out, "0000000002_add_c.up.sql"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(out, "ptah.sum"))
	c.Assert(err, qt.IsNil)
}

func TestImportCommand_DryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	src := writeGolangMigrateSource(t)
	out := t.TempDir()

	stdout, err := execute("--source-dir", src, "--migrations-dir", out, "--dry-run")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Dry run")

	entries, err := os.ReadDir(out)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func TestImportCommand_RequiresSourceDir(t *testing.T) {
	c := qt.New(t)
	_, err := execute("--migrations-dir", t.TempDir())
	c.Assert(err, qt.ErrorMatches, `.*--source-dir is required.*`)
}

func TestImportCommand_UnknownTool(t *testing.T) {
	c := qt.New(t)
	src := writeGolangMigrateSource(t)
	_, err := execute("--source-dir", src, "--migrations-dir", t.TempDir(), "--from", "nope")
	c.Assert(err, qt.ErrorMatches, `.*unsupported source tool "nope".*`)
}
