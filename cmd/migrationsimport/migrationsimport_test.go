package migrationsimport_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/migrationsimport"
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
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_init.up.sql":   "CREATE TABLE t (id int);\n",
		"1_init.down.sql": "DROP TABLE t;\n",
		"2_add_c.up.sql":  "ALTER TABLE t ADD c text;\n",
		"README.md":       "# migrations\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
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

// writeSourceWithADroppedMigration lays out a golang-migrate source whose second
// migration sits one directory down and whose third has a name off by one
// segment -- the two shapes that used to import silently short.
func writeSourceWithADroppedMigration(t *testing.T) string {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(dir, "tenant"), 0o755), qt.IsNil)
	files := map[string]string{
		"000001_create.up.sql":     "CREATE TABLE t (id INTEGER);\n",
		"000001_create.down.sql":   "DROP TABLE t;\n",
		"tenant/000002_add.up.sql": "ALTER TABLE t ADD c TEXT;\n",
		"000003_index.sql":         "CREATE INDEX i ON t (id);\n",
		"README.md":                "how to run these\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// The command refuses the import and names every file it did not convert.
//
// Before stokaro/ptah#2231 it printed "Wrote 2 migration file(s)", wrote
// ptah.sum over those two, and exited 0 -- so the truncated directory validated
// clean and nothing said the other SQL had been left behind.
func TestImportCommand_RefusesAPartialImportAndNamesWhatItDeclined(t *testing.T) {
	c := qt.New(t)
	source := writeSourceWithADroppedMigration(t)
	outDir := filepath.Join(t.TempDir(), "out")

	out, err := execute("--from", "golang-migrate", "--source-dir", source, "--migrations-dir", outDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "tenant/000002_add.up.sql")
	c.Assert(out, qt.Contains, "reads only the top level")
	c.Assert(out, qt.Contains, "000003_index.sql")
	c.Assert(out, qt.Contains, "--allow-partial")

	_, statErr := os.Stat(filepath.Join(outDir, "ptah.sum"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("ptah.sum must not certify a truncated import"))
}

// With the opt-in the import completes, and the declined files are still named.
func TestImportCommand_AllowPartialStillNamesTheDeclinedFiles(t *testing.T) {
	c := qt.New(t)
	source := writeSourceWithADroppedMigration(t)
	outDir := filepath.Join(t.TempDir(), "out")

	out, err := execute("--from", "golang-migrate", "--source-dir", source,
		"--migrations-dir", outDir, "--allow-partial")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Wrote 2 migration file(s)")
	c.Assert(out, qt.Contains, "Declined 3 source file(s)")
	// The README is reported alongside the two SQL files: the importer cannot
	// tell it from a migration whose name missed the rule, and only the caller
	// can. It is not what blocked the checksum, though -- the run above was
	// blocked by the two .sql files.
	c.Assert(out, qt.Contains, "README.md")

	_, statErr := os.Stat(filepath.Join(outDir, "ptah.sum"))
	c.Assert(statErr, qt.IsNil)
}

// A source holding nothing but migrations and a README imports without an opt-in.
//
// This is the control for the refusal: a guard that blocked on any declined file
// would stop almost every real migrations directory, and would be routed around
// rather than read.
func TestImportCommand_AReadmeAloneDoesNotBlockTheImport(t *testing.T) {
	c := qt.New(t)
	source := writeGolangMigrateSource(t)
	c.Assert(os.WriteFile(filepath.Join(source, "README.md"), []byte("notes\n"), 0o600), qt.IsNil)
	outDir := filepath.Join(t.TempDir(), "out")

	out, err := execute("--from", "golang-migrate", "--source-dir", source, "--migrations-dir", outDir)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "README.md")

	_, statErr := os.Stat(filepath.Join(outDir, "ptah.sum"))
	c.Assert(statErr, qt.IsNil)
}
