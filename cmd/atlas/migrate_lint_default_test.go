package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

var (
	atlasLintOkDurationRe    = regexp.MustCompile(`-- ok \([^)]+\)`)
	atlasLintTotalDurationRe = regexp.MustCompile(`(?m)^(  -------------------------\n  -- ).+$`)
)

// redactAtlasLintDurations replaces the non-deterministic elapsed durations in
// the default Atlas migrate lint text report with the literal "DUR" so the rest
// of the report can be asserted exactly.
func redactAtlasLintDurations(s string) string {
	s = atlasLintOkDurationRe.ReplaceAllString(s, "-- ok (DUR)")
	return atlasLintTotalDurationRe.ReplaceAllString(s, "${1}DUR")
}

func writeAtlasLintFile(c *qt.C, dir, name, body string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
}

func runAtlasMigrateLint(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestCompatCommand_MigrateLintDefaultTextClean(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "clean.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes until version 1 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 1\n"+
			"    -- no diagnostics found\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version ok\n"+
			"  -- 1 schema change\n")
}

func TestCompatCommand_MigrateLintDefaultTextDestructive(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "destructive.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "DROP TABLE users;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextDropColumn(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "drop-column.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE \"Users\" (id int PRIMARY KEY, \"Legacy\" text);\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE \"Users\" DROP COLUMN \"Legacy\";\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping non-virtual column \"Legacy\" https://atlasgo.io/lint/analyzers#DS103\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure column \"Legacy\" is NULL before dropping it\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextWrapsDropColumnAtMeasuredBoundary(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "drop-column-wrap.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int PRIMARY KEY, c1234567890123456 text);\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE users DROP COLUMN c1234567890123456;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping non-virtual column \"c1234567890123456\"\n"+
			"         https://atlasgo.io/lint/analyzers#DS103\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure column \"c1234567890123456\" is NULL before dropping\n"+
			"         it\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextNormalizesTypeAndIdentifiers(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "normalize.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE \"Users\" (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE \"Users\" ADD COLUMN \"Display Name\" VARCHAR(255) NOT NULL;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- data dependent changes detected:\n"+
			"      -- L1: Adding a non-nullable \"varchar\" column \"Display Name\" will fail in case table\n"+
			"         \"Users\" is not empty https://atlasgo.io/lint/analyzers#MF103\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with warnings\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextWrapsAnalyzerURLAtMeasuredBoundary(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "wrap.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE abcdefghijklmnopqrs (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "DROP TABLE abcdefghijklmnopqrs;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping table \"abcdefghijklmnopqrs\"\n"+
			"         https://atlasgo.io/lint/analyzers#DS102\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure table \"abcdefghijklmnopqrs\" is empty before\n"+
			"         dropping it\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextDoesNotFabricateAtlasLinks(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "unmapped.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE users RENAME COLUMN id TO oid;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "L1 [BC101]:")
	c.Assert(stdout, qt.Not(qt.Contains), "https://atlasgo.io/lint/analyzers#BC101")
}

func TestCompatCommand_MigrateLintDefaultTextDataDependentWarning(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "warning.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE users ADD COLUMN c2 int NOT NULL;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- data dependent changes detected:\n"+
			"      -- L1: Adding a non-nullable \"int\" column \"c2\" will fail in case table \"users\" is not empty\n"+
			"         https://atlasgo.io/lint/analyzers#MF103\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with warnings\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

// TestCompatCommand_MigrateLintDefaultTextAddNotNullFixture reproduces the
// imported Atlas cli-migrate-lint-add-notnull txtar fixture byte-for-byte: version 1
// adds a NOT NULL column to a table created in the same file (exempt), version 2
// adds one to the now-pre-existing table (MF103) and one with a DEFAULT (no
// report).
func TestCompatCommand_MigrateLintDefaultTextAddNotNullFixture(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "add-notnull.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n\n/* Adding a not-null column without default to a table created in this file should not report. */\nALTER TABLE users ADD COLUMN c1 int NOT NULL;\n")
	writeAtlasLintFile(c, dir, "2.sql", "ALTER TABLE users ADD COLUMN c2 int NOT NULL;\n\nALTER TABLE users ADD COLUMN c3 int NOT NULL DEFAULT 1;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "2")

	c.Assert(err, qt.IsNil)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes until version 2 (2 migrations in total):\n"+
			"\n"+
			"  -- analyzing version 1\n"+
			"    -- no diagnostics found\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- data dependent changes detected:\n"+
			"      -- L1: Adding a non-nullable \"int\" column \"c2\" will fail in case table \"users\" is not empty\n"+
			"         https://atlasgo.io/lint/analyzers#MF103\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version ok, 1 with warnings\n"+
			"  -- 4 schema changes\n"+
			"  -- 1 diagnostic\n")
}

func TestCompatCommand_MigrateLintDefaultTextInlineSuppressed(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "suppressed.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\nCREATE TABLE pets (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", "\n-- atlas:nolint\nALTER TABLE users ADD COLUMN name text NOT NULL;\n\n-- atlas:nolint\nDROP TABLE pets;\n")

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- no diagnostics found\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version ok\n"+
			"  -- 2 schema changes\n")
}

// atlasProjectLintLogConfig mirrors the imported Atlas cli-migrate-lint-project
// fixture: a global lint.log-free analysis policy plus per-env lint.log
// templates that render the migrate lint output.
const atlasProjectLintLogConfig = `lint {
  latest = 1
  destructive {
    error = false
  }
}

env "log_name" {
  lint {
    log = "{{ range .Files }}{{ println .Name }}{{ end }}"
  }
}

env "log_count" {
  lint {
    latest = 2
    log = "{{ len .Files | println }}"
  }
}
`

func TestCompatCommand_MigrateLintProjectGlobalLintLog(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "1.sql", "CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN id TO oid;\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "2.sql", "DROP TABLE users;\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "3.sql", "DROP TABLE pets;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(atlasProjectLintLogConfig), 0o600), qt.IsNil)
	t.Chdir(dir)

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", "migrations", "--env", "log_name")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "3.sql\n")
}

func TestCompatCommand_MigrateLintProjectEnvLintLog(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "1.sql", "CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN id TO oid;\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "2.sql", "DROP TABLE users;\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "3.sql", "DROP TABLE pets;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(atlasProjectLintLogConfig), 0o600), qt.IsNil)
	t.Chdir(dir)

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", "migrations", "--env", "log_count")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "2\n")
}
