package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin stokaro/ptah#1013 section 1 on `ptah-compat migrate lint`: a
// migration directory laid out in a foreign tool's convention is read through
// both spellings that select it, converted in memory, and analyzed — instead of
// being refused.
//
// Before this change every row here printed one of two refusals:
//
//	atlas migrate lint --dir: Atlas accepts ?format=golang-migrate, but Ptah
//	  does not implement that directory format for this command yet
//	atlas migrate lint --dir-format: Atlas accepts --dir-format=golang-migrate,
//	  but Ptah does not implement that directory format yet
//
// except TestCompatMigrateLint_RejectsNonVerbatimDirFormat, which used to exit 0
// and now exits 1, and the control rows, which are unchanged by design.
//
// Measured against the pinned community binary v1.3.0 on the same fixtures,
// 2026-08-03. The golang-migrate fixture is what makes the flip visible: its
// atlas.sum covers only 1_init.up.sql, so read as a native Atlas directory it is
// a checksum mismatch. `migrate lint --dir 'file://gm?format=golang-migrate'
// --dev-url ... --latest 1` exits 0 there with `-- 1 version ok`, while the same
// directory with no query exits 1 with
// `checksum mismatch (atlas.sum): L2: 1_init.down.sql was added`.

const (
	lintConvertedUp   = "1_init.up.sql"
	lintConvertedDown = "1_init.down.sql"
)

// writeLintGolangMigrateDir writes a golang-migrate pair and the atlas.sum that
// layout covers — the up file only.
//
// The sum goes through `migrate hash` rather than a helper so the bytes these
// rows verify against are the ones the shipped verb writes (#984, #992), and so
// a change to the covered set shows up here rather than being encoded twice.
func writeLintGolangMigrateDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	writeAtlasApplyProjectMigration(c.TB, dir, lintConvertedUp, "CREATE TABLE g1 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, dir, lintConvertedDown, "DROP TABLE g1;\n")
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	return dir
}

// writeLintFlywayNestedDir writes a Flyway directory whose second migration sits
// one level down, and hashes it for the Flyway layout.
//
// Flyway is the only layout whose atlas.sum reaches below the top level, so this
// fixture separates "the covered set is resolved for the SOURCE layout" from
// "the covered set is the Atlas top-level rule applied to whatever was
// captured". The second reading covers V1__init.sql alone and reports
// sub/V2__nested.sql as removed; the community binary exits 0 with two versions
// analyzed.
func writeLintFlywayNestedDir(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	writeAtlasApplyProjectMigration(c.TB, dir, "V1__init.sql", "CREATE TABLE f1 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, filepath.Join(dir, "sub"), "V2__nested.sql",
		"CREATE TABLE f2 (id INTEGER PRIMARY KEY);\n")
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	return dir
}

// lintDevURL returns a dev-database URL for a file that does not exist yet, so
// each row replays into an empty database.
func lintDevURL(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
}

// runLint invokes `migrate lint --latest 1` over dir with the extra arguments a
// row supplies.
func runLint(tb testing.TB, dir string, extra []string, latest string) (stdout, stderr string, err error) {
	c := qt.New(tb)
	c.Helper()
	args := append([]string{"migrate", "lint", "--dir", "file://" + dir}, extra...)
	args = append(args, "--dev-url", lintDevURL(c.TB), "--latest", latest)
	return runCompatExit(args...)
}

func TestCompatMigrateLint_FlywayReportUsesExactSourceTokens(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c.TB, []setFlywayMigration{
		{name: "V1__a.sql", body: "CREATE TABLE a (id INTEGER PRIMARY KEY);\n"},
		{name: "V1.5__b.sql", body: "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__c.sql", body: "CREATE TABLE c (id INTEGER PRIMARY KEY);\n"},
	})

	stdout, stderr, err := runLint(c.TB, dir, []string{"--dir-format", "flyway"}, "1")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "Analyzing changes from version 1.5 to 2 (1 migration in total):")
	c.Assert(stdout, qt.Contains, "-- analyzing version 2")
}

func TestCompatMigrateLint_FlywayReplayFailureUsesExactSourceToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c.TB, []setFlywayMigration{
		{name: "V1.5__broken.sql", body: "INSERT INTO missing_lint_table VALUES (1);\n"},
	})

	stdout, stderr, err := runLint(c.TB, dir, []string{"--dir-format", "flyway"}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout+stderr+errorText(err), qt.Contains, "replay migration 1.5")
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "461168")

	stdout, stderr, err = runLint(c.TB, dir, []string{
		"--dir-format", "flyway",
		"--format", `{{ range .Steps }}{{ .Error }}{{ end }}`,
	}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, "replay migration 1.5")
	c.Assert(stdout, qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stdout, qt.Not(qt.Contains), "461168")
	c.Assert(stderr, qt.Equals, "")
}

func TestCompatMigrateLint_FlywayReplayFailureNamesExactEmptySourceToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c.TB, []setFlywayMigration{{
		name: "V.sql",
		body: "INSERT INTO missing_empty_lint_table VALUES (1);\n",
	}})

	stdout, stderr, err := runLint(c.TB, dir, []string{"--dir-format", "flyway"}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout+stderr+errorText(err), qt.Contains, `replay migration "" on dev database`)
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "replay migration  on dev database")

	stdout, stderr, err = runLint(c.TB, dir, []string{
		"--dir-format", "flyway",
		"--format", `{{ range .Steps }}{{ .Error }}{{ end }}`,
	}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, `replay migration "" on dev database`)
	c.Assert(stdout, qt.Not(qt.Contains), "replay migration  on dev database")
	c.Assert(stderr, qt.Equals, "")
}

// TestCompatMigrateLint_ConvertedDirIsRead is the discriminator for #1013
// section 1: both spellings that select a foreign layout make lint analyze the
// converted directory, and the no-selection control shows the outcome genuinely
// flips rather than the directory having been lintable all along.
//
// Reverted, the two selection rows print `Ptah does not implement that directory
// format`; the control row prints `checksum mismatch` before and after.
func TestCompatMigrateLint_ConvertedDirIsRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		suffix string
		extra  []string
		assert func(c *qt.C, stdout, stderr string, err error)
	}{
		{
			name:   "dir_query",
			suffix: "?format=golang-migrate",
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
				c.Assert(stdout, qt.Contains, "1 version ok")
			},
		},
		{
			name:  "dir_format_flag",
			extra: []string{"--dir-format", "golang-migrate"},
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
				c.Assert(stdout, qt.Contains, "1 version ok")
			},
		},
		{
			name: "control_no_selection_is_a_checksum_mismatch",
			assert: func(c *qt.C, _, stderr string, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(stderr, qt.Contains, "checksum mismatch")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeLintGolangMigrateDir(c)

			stdout, stderr, err := runLint(c.TB, dir+test.suffix, test.extra, "1")

			test.assert(c, stdout, stderr, err)
		})
	}
}

// TestCompatMigrateLint_QueryFormatOutranksDirFormatFlag pins the precedence the
// issue's definition of done names, in BOTH directions.
//
// One direction alone cannot tell precedence from "the flag is ignored" or from
// "the query is ignored": each row's two spellings name different layouts, and
// the outcome is only reachable if the query's layout is the one that ran. The
// golang-migrate row exits 0 only if the down file was left out of the covered
// set; the atlas row exits 0 only if it was not.
//
// Reverted, both rows print `Ptah does not implement that directory format`.
func TestCompatMigrateLint_QueryFormatOutranksDirFormatFlag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		suffix string
		extra  []string
		dir    func(c *qt.C) string
	}{
		{
			name:   "query_golang_migrate_beats_flag_atlas",
			suffix: "?format=golang-migrate",
			extra:  []string{"--dir-format", "atlas"},
			dir:    writeLintGolangMigrateDir,
		},
		{
			name:   "query_atlas_beats_flag_golang_migrate",
			suffix: "?format=atlas",
			extra:  []string{"--dir-format", "golang-migrate"},
			dir:    writeLintNativeAtlasDir,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := test.dir(c)

			stdout, stderr, err := runLint(c.TB, dir+test.suffix, test.extra, "1")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, "1 version ok")
		})
	}
}

// writeLintNativeAtlasDir writes a one-migration native Atlas directory and its
// atlas.sum. Read as golang-migrate its covered set is empty, so it is the half
// of the precedence pair that can only pass if `?format=atlas` won.
func writeLintNativeAtlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	writeAtlasApplyProjectMigration(c.TB, dir, "20240101000000_init.sql",
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n")
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	return dir
}

// TestCompatMigrateLint_ConvertedIntegrityUsesTheSourceLayoutCoveredSet pins
// which files lint's checksum step hashes once a foreign layout is selected.
//
// This is the row set that separates the fix from two cheaper ones that would
// also make the flip test above pass. Both were run as mutants against this
// file, and each row named below is one that went red:
//
//   - Resolving the covered set under the Atlas rule instead of the source
//     layout's — SumFileNames(gateFS, FormatAtlas) — takes down
//     editing_an_uncovered_down_file_is_not (the down file becomes covered) and
//     a_nested_flyway_migration_is_covered_and_analyzed (the nested file stops
//     being covered and reads as removed).
//   - Dropping the checksum outcome on the way to the report takes down
//     editing_a_covered_up_file_is_a_mismatch and
//     editing_a_nested_flyway_migration_is_a_mismatch.
//
// Reverted, every row prints `Ptah does not implement that directory format`.
func TestCompatMigrateLint_ConvertedIntegrityUsesTheSourceLayoutCoveredSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		latest string
		setup  func(c *qt.C) (dir, suffix string)
		assert func(c *qt.C, stdout, stderr string, err error)
	}{
		{
			name:   "editing_a_covered_up_file_is_a_mismatch",
			latest: "1",
			setup: func(c *qt.C) (string, string) {
				dir := writeLintGolangMigrateDir(c)
				writeAtlasApplyProjectMigration(c.TB, dir, lintConvertedUp,
					"CREATE TABLE g1 (id INTEGER PRIMARY KEY, extra INTEGER);\n")
				return dir, "?format=golang-migrate"
			},
			assert: func(c *qt.C, _, stderr string, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(stderr, qt.Contains, "checksum mismatch")
			},
		},
		{
			name:   "editing_an_uncovered_down_file_is_not",
			latest: "1",
			setup: func(c *qt.C) (string, string) {
				dir := writeLintGolangMigrateDir(c)
				writeAtlasApplyProjectMigration(c.TB, dir, lintConvertedDown, "DROP TABLE IF EXISTS g1;\n")
				return dir, "?format=golang-migrate"
			},
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
				c.Assert(stdout, qt.Contains, "1 version ok")
			},
		},
		{
			name:   "a_nested_flyway_migration_is_covered_and_analyzed",
			latest: "2",
			setup: func(c *qt.C) (string, string) {
				return writeLintFlywayNestedDir(c.TB), "?format=flyway"
			},
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
				c.Assert(stdout, qt.Contains, "2 versions ok")
			},
		},
		{
			name:   "editing_a_nested_flyway_migration_is_a_mismatch",
			latest: "2",
			setup: func(c *qt.C) (string, string) {
				dir := writeLintFlywayNestedDir(c.TB)
				writeAtlasApplyProjectMigration(c.TB, filepath.Join(dir, "sub"), "V2__nested.sql",
					"CREATE TABLE f2 (id INTEGER PRIMARY KEY, extra INTEGER);\n")
				return dir, "?format=flyway"
			},
			assert: func(c *qt.C, _, stderr string, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(stderr, qt.Contains, "checksum mismatch")
			},
		},
		{
			name:   "an_unhashed_converted_directory_is_analyzed",
			latest: "1",
			setup: func(c *qt.C) (string, string) {
				dir := writeLintGolangMigrateDir(c)
				c.Assert(os.Remove(filepath.Join(dir, "atlas.sum")), qt.IsNil)
				return dir, "?format=golang-migrate"
			},
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
				c.Assert(stdout, qt.Contains, "1 version ok")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, suffix := test.setup(c)

			stdout, stderr, err := runLint(c.TB, dir+suffix, nil, test.latest)

			test.assert(c, stdout, stderr, err)
		})
	}
}

// TestCompatMigrateLint_RejectsNonVerbatimDirFormat is the one row here that
// makes lint STRICTER, and it closes a live violation of the rule that
// ptah-compat must never exit 0 where the community binary exits 1.
//
// Measured on a clean, hashed native Atlas directory: the community binary exits
// 1 with `unknown dir format "ATLAS"` and `unknown dir format " atlas "`, while
// ptah-compat exited 0 and linted, because the flag went through
// atlasMigrateDirFormatValue's lower-and-trim normalization. Routing lint
// through the shared verbatim resolver removes the normalization.
//
// Reverted, both rows print the lint report and a nil error. Restoring the
// normalization inside the shared resolver instead — the only place it could
// come back now — takes down exactly these two rows and nothing else.
func TestCompatMigrateLint_RejectsNonVerbatimDirFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
	}{
		{name: "uppercase", value: "ATLAS"},
		{name: "padded", value: " atlas "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeLintNativeAtlasDir(c)

			_, _, err := runLint(c.TB, dir, []string{"--dir-format", test.value}, "1")

			c.Assert(err, qt.ErrorMatches, `unknown dir format ".*"`)
		})
	}
}

// TestCompatMigrateLint_EmptyDirFormatStillReadsAtlas guards the row above from
// over-refusing. An empty `--dir-format` selects the native Atlas layout on the
// community binary — measured exit 0 on the same fixture — so the verbatim
// resolver must not treat it as an unknown value.
//
// Reverted, this prints the same lint report; it is a non-interference control
// for the strictness change, not a behavior change of its own. Reverting proves
// nothing about a control — removing a guard never makes it fire — so it was
// checked with the INVERSE mutant instead: making the resolver refuse an empty
// value as well takes this row and
// TestCompatMigrateDirQuery_EmptyFormatValueReadsAtlasLayout red, and nothing
// else.
func TestCompatMigrateLint_EmptyDirFormatStillReadsAtlas(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeLintNativeAtlasDir(c)

	stdout, stderr, err := runLint(c.TB, dir, []string{"--dir-format", ""}, "1")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "1 version ok")
}
