package migrationsimport_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrationsimport"
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
// segment -- the two shapes an unreporting import takes silently short.
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
// An import that printed "Wrote 2 migration file(s)", wrote ptah.sum over those
// two and exited 0 would leave the truncated directory validating clean, with
// nothing saying the other SQL had been left behind.
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

// writeLiquibaseTypedSource lays out a Liquibase XML changelog whose changeset
// is a typed change, which has no SQL until a dialect is chosen.
func writeLiquibaseTypedSource(t *testing.T) string {
	c := qt.New(t)
	dir := t.TempDir()
	changelog := `<databaseChangeLog><changeSet id="1" author="simon">` +
		`<createTable tableName="widgets"><column name="id" type="bigint">` +
		`<constraints primaryKey="true"/></column></createTable>` +
		`</changeSet></databaseChangeLog>`
	c.Assert(os.WriteFile(filepath.Join(dir, "changelog.xml"), []byte(changelog), 0o600), qt.IsNil)
	return dir
}

// --dialect reaches a detected parser: no --from is passed, and the typed
// change is rendered for the dialect named.
func TestImportCommand_DialectRendersLiquibaseTypedChanges(t *testing.T) {
	c := qt.New(t)
	src := writeLiquibaseTypedSource(t)
	out := t.TempDir()

	_, err := execute("--source-dir", src, "--migrations-dir", out, "--dialect", "postgres")
	c.Assert(err, qt.IsNil)

	up, err := os.ReadFile(filepath.Join(out, "0000000001_simon_1.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, `CREATE TABLE "widgets" (`)
	down, err := os.ReadFile(filepath.Join(out, "0000000001_simon_1.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, `DROP TABLE "widgets";`)
}

// The control for the test above: the same source without --dialect is
// refused, and the refusal names the flag.
func TestImportCommand_LiquibaseTypedChangeWithoutDialectIsRefused(t *testing.T) {
	c := qt.New(t)
	src := writeLiquibaseTypedSource(t)
	out := t.TempDir()

	_, err := execute("--source-dir", src, "--migrations-dir", out)

	c.Assert(err, qt.ErrorMatches, `(?s).*uses <createTable>, which is not SQL text.*pass --dialect.*`)
	entries, readErr := os.ReadDir(out)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// A dialect names how to render a change that has no SQL, and only a Liquibase
// source has one. Handed to a tool whose migrations are SQL, it is refused
// before anything is written rather than ignored.
func TestImportCommand_DialectIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		message string
	}{
		{name: "for a SQL source", dialect: "postgres", message: `(?s).*--dialect: a target dialect applies only to a Liquibase source; golang-migrate migrations are SQL already.*`},
		{name: "when unknown", dialect: "db2", message: `(?s).*--dialect: .*unsupported dialect "db2".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			src := writeGolangMigrateSource(t)
			out := t.TempDir()

			_, err := execute("--source-dir", src, "--migrations-dir", out, "--dialect", test.dialect)

			c.Assert(err, qt.ErrorMatches, test.message)
			entries, readErr := os.ReadDir(out)
			c.Assert(readErr, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

// --server-version refines the preset --dialect selects, so it means nothing
// alone, and a value that names another server product is refused rather than
// planned against the wrong one. Neither writes anything.
func TestImportCommand_ServerVersionIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		message string
	}{
		{
			name:    "without a dialect",
			args:    []string{"--server-version", "17"},
			message: `(?s).*--server-version requires --dialect.*`,
		},
		{
			name:    "naming another server",
			args:    []string{"--dialect", "postgres", "--server-version", "10.11.6-MariaDB"},
			message: `(?s).*invalid --server-version: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			src := writeLiquibaseTypedSource(t)
			out := t.TempDir()

			_, err := execute(append([]string{"--source-dir", src, "--migrations-dir", out}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, test.message)
			entries, readErr := os.ReadDir(out)
			c.Assert(readErr, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

// A recognized release line converts the typed change the way the dialect
// alone does.
func TestImportCommand_ServerVersionRendersForTheNamedLine(t *testing.T) {
	c := qt.New(t)
	src := writeLiquibaseTypedSource(t)
	out := t.TempDir()

	_, err := execute("--source-dir", src, "--migrations-dir", out, "--dialect", "postgres", "--server-version", "17")
	c.Assert(err, qt.IsNil)

	up, err := os.ReadFile(filepath.Join(out, "0000000001_simon_1.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, `CREATE TABLE "widgets" (`)
}
