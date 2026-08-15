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

// The layout names a row selects a directory with, in the spelling both the
// `?format=` query and `--dir-format` take.
const (
	lintFormatGolangMigrate = "golang-migrate"
	lintFormatFlyway        = "flyway"
	lintFormatAtlas         = "atlas"
)

// lintSourceFixtures is the migration set each fixture holds, keyed by the
// layout its atlas.sum is written for. A row names a layout and gets both, so
// what a fixture contains stays readable as data instead of living in a builder
// per layout.
//
// The golang-migrate pair is the one whose atlas.sum covers the up file only,
// which is what makes the down file the uncovered half.
//
// The Flyway fixture puts its second migration one level down. Flyway is the
// only layout whose atlas.sum reaches below the top level, so it separates "the
// covered set is resolved for the SOURCE layout" from "the covered set is the
// Atlas top-level rule applied to whatever was captured". The second reading
// covers V1__init.sql alone and reports sub/V2__nested.sql as removed; the
// community binary exits 0 with two versions analyzed.
//
// The native Atlas fixture read as golang-migrate has an empty covered set,
// which is what makes it the half of the precedence pair that can only pass if
// `?format=atlas` won.
var lintSourceFixtures = map[string]map[string]string{
	lintFormatGolangMigrate: {
		lintConvertedUp:   "CREATE TABLE g1 (id INTEGER PRIMARY KEY);\n",
		lintConvertedDown: "DROP TABLE g1;\n",
	},
	lintFormatFlyway: {
		"V1__init.sql":       "CREATE TABLE f1 (id INTEGER PRIMARY KEY);\n",
		"sub/V2__nested.sql": "CREATE TABLE f2 (id INTEGER PRIMARY KEY);\n",
	},
	lintFormatAtlas: {
		"20240101000000_init.sql": "CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n",
	},
}

// writeHashedLintDir writes the fixture for format and hashes it AS that
// layout, which is what makes its covered set the source layout's own.
//
// The sum goes through `migrate hash` rather than a helper so the bytes these
// rows verify against are the ones the shipped verb writes (#984, #992), and so
// a change to the covered set shows up here rather than being encoded twice.
func writeHashedLintDir(c *qt.C, format string) string {
	c.Helper()
	dir := c.TempDir()
	writeLintMigrations(c, dir, lintSourceFixtures[format])
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+format)
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	return dir
}

// writeLintMigrations writes each migration at its own path below dir, so a
// fixture can name the nested file only the Flyway layout has.
func writeLintMigrations(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	for name, body := range files {
		writeAtlasApplyProjectMigration(c, filepath.Join(dir, filepath.Dir(name)), filepath.Base(name), body)
	}
}

// editLintDir rewrites and removes files after the directory was hashed, which
// is how a row moves a file out from under the checksum that covered it.
func editLintDir(c *qt.C, dir string, edits map[string]string, removed []string) {
	c.Helper()
	writeLintMigrations(c, dir, edits)
	for _, name := range removed {
		c.Assert(os.Remove(filepath.Join(dir, filepath.FromSlash(name))), qt.IsNil)
	}
}

// writeLintGolangMigrateDir is the golang-migrate fixture other files reach for
// by name.
func writeLintGolangMigrateDir(c *qt.C) string {
	c.Helper()
	return writeHashedLintDir(c, lintFormatGolangMigrate)
}

// lintDevURL returns a dev-database URL for a file that does not exist yet, so
// each row replays into an empty database.
func lintDevURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
}

// runLint invokes `migrate lint --latest 1` over dir with the extra arguments a
// row supplies.
func runLint(c *qt.C, dir string, extra []string, latest string) (stdout, stderr string, err error) {
	c.Helper()
	args := append([]string{"migrate", "lint", "--dir", "file://" + dir}, extra...)
	args = append(args, "--dev-url", lintDevURL(c), "--latest", latest)
	return runCompatExit(args...)
}

func TestCompatMigrateLint_FlywayReportUsesExactSourceTokens(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1__a.sql", body: "CREATE TABLE a (id INTEGER PRIMARY KEY);\n"},
		{name: "V1.5__b.sql", body: "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__c.sql", body: "CREATE TABLE c (id INTEGER PRIMARY KEY);\n"},
	})

	stdout, stderr, err := runLint(c, dir, []string{"--dir-format", "flyway"}, "1")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "Analyzing changes from version 1.5 to 2 (1 migration in total):")
	c.Assert(stdout, qt.Contains, "-- analyzing version 2")
}

func TestCompatMigrateLint_FlywayReplayFailureUsesExactSourceToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__broken.sql", body: "INSERT INTO missing_lint_table VALUES (1);\n"},
	})

	stdout, stderr, err := runLint(c, dir, []string{"--dir-format", "flyway"}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout+stderr+errorText(err), qt.Contains, "replay migration 1.5")
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "461168")

	stdout, stderr, err = runLint(c, dir, []string{
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
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: "V.sql",
		body: "INSERT INTO missing_empty_lint_table VALUES (1);\n",
	}})

	stdout, stderr, err := runLint(c, dir, []string{"--dir-format", "flyway"}, "1")
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout+stderr+errorText(err), qt.Contains, `replay migration "" on dev database`)
	c.Assert(stdout+stderr+errorText(err), qt.Not(qt.Contains), "replay migration  on dev database")

	stdout, stderr, err = runLint(c, dir, []string{
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
// converted directory.
//
// The no-selection control is
// [TestCompatMigrateLint_UnselectedForeignDirIsAChecksumMismatch]: it asserts a
// refusal rather than a report, so it is a test of its own.
//
// Reverted, the two rows here print `Ptah does not implement that directory
// format`; the control prints `checksum mismatch` before and after.
func TestCompatMigrateLint_ConvertedDirIsRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		suffix string
		extra  []string
	}{
		{
			name:   "dir_query",
			suffix: "?format=golang-migrate",
		},
		{
			name:  "dir_format_flag",
			extra: []string{"--dir-format", "golang-migrate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeLintGolangMigrateDir(c)

			stdout, stderr, err := runLint(c, dir+test.suffix, test.extra, "1")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, "1 version ok")
		})
	}
}

// TestCompatMigrateLint_UnselectedForeignDirIsAChecksumMismatch is the control
// for the test above: with no layout selected, the same directory is read as a
// native Atlas one, and the atlas.sum written for the golang-migrate layout
// does not describe it. Without this, "lint exits 0 on the converted directory"
// would also be satisfied by a directory that was lintable all along.
func TestCompatMigrateLint_UnselectedForeignDirIsAChecksumMismatch(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeLintGolangMigrateDir(c)

	_, stderr, err := runLint(c, dir, nil, "1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "checksum mismatch")
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
		name string
		// fixture is the layout the directory is written and hashed as. The
		// query names it and the flag contradicts it, so the row passes only if
		// the query decided.
		fixture string
		suffix  string
		extra   []string
	}{
		{
			name:    "query_golang_migrate_beats_flag_atlas",
			fixture: lintFormatGolangMigrate,
			suffix:  "?format=golang-migrate",
			extra:   []string{"--dir-format", "atlas"},
		},
		{
			name:    "query_atlas_beats_flag_golang_migrate",
			fixture: lintFormatAtlas,
			suffix:  "?format=atlas",
			extra:   []string{"--dir-format", "golang-migrate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedLintDir(c, test.fixture)

			stdout, stderr, err := runLint(c, dir+test.suffix, test.extra, "1")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, "1 version ok")
		})
	}
}

// writeLintNativeAtlasDir is the native Atlas fixture, hashed for its own
// layout.
func writeLintNativeAtlasDir(c *qt.C) string {
	c.Helper()
	return writeHashedLintDir(c, lintFormatAtlas)
}

// TestCompatMigrateLint_ConvertedIntegrityRefusesAnEditedCoveredFile pins which
// files lint's checksum step hashes once a foreign layout is selected, from the
// side where the answer is a refusal. The other side -- what the covered set
// leaves out, which must still analyze clean -- is
// [TestCompatMigrateLint_ConvertedIntegrityAnalyzesWhatTheCoveredSetLeavesOut].
//
// The two together are the row set that separates the fix from two cheaper ones
// that would also make the flip test above pass. Both were run as mutants
// against this file, and each row named below is one that went red:
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
// Reverted, every row of both tests prints `Ptah does not implement that
// directory format`.
func TestCompatMigrateLint_ConvertedIntegrityRefusesAnEditedCoveredFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		format string
		latest string
		// edits are written after the directory was hashed, which is what moves
		// a covered file out from under the checksum that covered it.
		edits map[string]string
	}{
		{
			name:   "editing_a_covered_up_file_is_a_mismatch",
			format: lintFormatGolangMigrate,
			latest: "1",
			edits: map[string]string{
				lintConvertedUp: "CREATE TABLE g1 (id INTEGER PRIMARY KEY, extra INTEGER);\n",
			},
		},
		{
			name:   "editing_a_nested_flyway_migration_is_a_mismatch",
			format: lintFormatFlyway,
			latest: "2",
			edits: map[string]string{
				"sub/V2__nested.sql": "CREATE TABLE f2 (id INTEGER PRIMARY KEY, extra INTEGER);\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedLintDir(c, test.format)
			editLintDir(c, dir, test.edits, nil)

			_, stderr, err := runLint(c, dir+"?format="+test.format, nil, test.latest)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Contains, "checksum mismatch")
		})
	}
}

// TestCompatMigrateLint_ConvertedIntegrityAnalyzesWhatTheCoveredSetLeavesOut is
// the accepting half of
// [TestCompatMigrateLint_ConvertedIntegrityRefusesAnEditedCoveredFile]: a file
// the source layout's covered set does not name may change, and a directory
// with no atlas.sum at all is still analyzed, because neither is a checksum
// this command is entitled to refuse.
func TestCompatMigrateLint_ConvertedIntegrityAnalyzesWhatTheCoveredSetLeavesOut(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		format  string
		latest  string
		edits   map[string]string
		removed []string
		// wantAnalyzed also pins HOW MANY versions were read, which is what
		// separates a nested migration that is analyzed from one that is
		// silently left out of the report.
		wantAnalyzed string
	}{
		{
			name:         "editing_an_uncovered_down_file_is_not",
			format:       lintFormatGolangMigrate,
			latest:       "1",
			edits:        map[string]string{lintConvertedDown: "DROP TABLE IF EXISTS g1;\n"},
			wantAnalyzed: "1 version ok",
		},
		{
			name:         "a_nested_flyway_migration_is_covered_and_analyzed",
			format:       lintFormatFlyway,
			latest:       "2",
			wantAnalyzed: "2 versions ok",
		},
		{
			name:         "an_unhashed_converted_directory_is_analyzed",
			format:       lintFormatGolangMigrate,
			latest:       "1",
			removed:      []string{"atlas.sum"},
			wantAnalyzed: "1 version ok",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedLintDir(c, test.format)
			editLintDir(c, dir, test.edits, test.removed)

			stdout, stderr, err := runLint(c, dir+"?format="+test.format, nil, test.latest)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, test.wantAnalyzed)
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

			_, _, err := runLint(c, dir, []string{"--dir-format", test.value}, "1")

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

	stdout, stderr, err := runLint(c, dir, []string{"--dir-format", ""}, "1")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "1 version ok")
}
