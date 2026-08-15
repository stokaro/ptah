package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrateApplyHonorsProjectBaseline pins stokaro/ptah#934 item 5a: the
// atlas.hcl `migration { baseline }` attribute is the config spelling of
// --baseline, and it now marks that version applied instead of being dropped
// with a warning.
//
// Measured on the pinned Atlas community binary v1.3.0 against a hashed
// two-migration directory, `migrate apply --env local --dry-run`, exit codes
// read directly from unpiped invocations:
//
//	no baseline                  0  "Migrating to version 20260719010101
//	                                (2 migrations in total)"
//	baseline = "20260719010000"  0  "Migrating to version 20260719010101 from
//	                                20260719010000 (1 migrations in total)"
//
// ptah-compat answered "2 pending migrations" to BOTH before the wiring, at
// exit 0, with `warning: atlas.hcl attribute "baseline" ... has no effect`.
//
// The no-baseline row is carried here as the control subtest rather than as
// prose. Both binaries agree there, so a run that applied one migration in both
// rows -- or two in both -- would fail this test, which is what stops it from
// passing for a reason other than the attribute.
//
// The assertion is on the DATABASE, not on the report: the baselined migration
// must not have run, and the pending one must have. A dry run would only show
// what Ptah intended.
func TestMigrateApplyHonorsProjectBaseline(t *testing.T) {
	tests := []struct {
		name           string
		migrationExtra string
		wantFirstRan   int
		wantSecondRan  int
	}{
		{
			// The control. Nothing is baselined, so both migrations execute.
			name:           "no baseline applies every migration",
			migrationExtra: "",
			wantFirstRan:   1,
			wantSecondRan:  1,
		},
		{
			name:           "a configured baseline skips the version it names",
			migrationExtra: "    baseline = \"1\"\n",
			wantFirstRan:   0,
			wantSecondRan:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeAtlasApplyProjectMigration(c, "migrations", "1_create_first.sql",
				"CREATE TABLE baseline_first (id INTEGER PRIMARY KEY);\n")
			writeAtlasApplyProjectMigration(c, "migrations", "2_create_second.sql",
				"CREATE TABLE baseline_second (id INTEGER PRIMARY KEY);\n")
			writeAtlasApplyProjectSum(c, "migrations")
			dbPath := filepath.Join(root, "apply.db")
			writeAtlasApplyBaselineProjectConfig(c, dbPath, test.migrationExtra)

			output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
			c.Check(sqliteTableCount(c, dbPath, "baseline_first"), qt.Equals, test.wantFirstRan)
			c.Check(sqliteTableCount(c, dbPath, "baseline_second"), qt.Equals, test.wantSecondRan)
		})
	}
}

// TestMigrateApplyBaselineFlagWinsOverProject keeps the precedence the merge
// helper is built on: --baseline is what the caller typed, so it overrides the
// project file. Measured on the pinned binary, byte-identical on both:
//
//	migration { baseline = "20260719010000" } with --baseline 20260719010101
//	  ->  0  "No migration files to execute"
func TestMigrateApplyBaselineFlagWinsOverProject(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c, "migrations", "1_create_first.sql",
		"CREATE TABLE baseline_first (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c, "migrations", "2_create_second.sql",
		"CREATE TABLE baseline_second (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyBaselineProjectConfig(c, dbPath, "    baseline = \"1\"\n")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local", "--baseline", "2")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	// The flag baselined version 2, so neither file ran. Had the project value
	// won, baseline_second would exist.
	c.Check(sqliteTableCount(c, dbPath, "baseline_first"), qt.Equals, 0)
	c.Check(sqliteTableCount(c, dbPath, "baseline_second"), qt.Equals, 0)
}

// TestMigrateApplyProjectBaselineNotFound pins the refusal, which is the arm
// that shows the value is resolved against the directory rather than stored.
// Measured with `migrate apply --env local` against a hashed directory that
// does not hold the named version, exit codes read directly from unpiped
// invocations:
//
//	pinned binary:  1  Error: baseline version "20200101000000" not found
//	ptah-compat:    1  Error: error baselining migrations: baseline version
//	                          "20200101000000" not found
//
// The wrapping prefix is not this change's: the SAME two lines come out of
// `--baseline 20200101000000` on a live apply, measured with no atlas.hcl
// baseline at all, and both binaries answer the bare sentence under --dry-run.
// The invariant worth pinning here is that the config spelling reaches exactly
// the flag's resolution, so the assertion matches the flag's own text.
func TestMigrateApplyProjectBaselineNotFound(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c, "migrations", "1_create_first.sql",
		"CREATE TABLE baseline_first (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyBaselineProjectConfig(c, dbPath, "    baseline = \"20200101000000\"\n")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.ErrorMatches, `.*baseline version "20200101000000" not found`,
		qt.Commentf("command output:\n%s", output))
}

// writeAtlasApplyBaselineProjectConfig writes an atlas.hcl whose migration block
// carries the given extra attribute lines verbatim. The caller supplies the
// whole `baseline = ...` line, or nothing at all for the control shape: an
// absent attribute is what the control needs, and `baseline = ""` is a
// different fixture.
func writeAtlasApplyBaselineProjectConfig(c *qt.C, dbPath, migrationExtra string) {
	c.Helper()
	c.Assert(os.WriteFile("atlas.hcl", fmt.Appendf(nil, `env "local" {
  url = "sqlite://%s"
  migration {
    dir     = "file://migrations"
    tx_mode = "file"
%s  }
}
`, filepath.ToSlash(dbPath), migrationExtra), 0o600), qt.IsNil)
}
