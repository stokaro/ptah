package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// Every test in this file drives the whole compatibility CLI, argv in and exit
// code out, because the exit code is the property under test. stokaro/ptah#1231
// is a list of invocations where `ptah-compat` exited 0 and the pinned Atlas
// community binary v1.3.0 exited 1; a test that called the checking function
// directly would pass while the command that has to refuse still returns 0,
// which is exactly how these gaps survived.
//
// The refusal text of each row was measured on that binary on 2026-08-08 and is
// quoted in the test that reproduces it. Where Ptah deliberately says something
// else, the test says so and says why.

// runAtlasPrecondition executes one compatibility invocation and returns
// everything it wrote plus the error carrying its exit code.
func runAtlasPrecondition(c *qt.C, args ...string) (output string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err = executeAtlasTestCommand(cmd)
	return out.String(), err
}

// seedAtlasPreconditionDir writes a two-migration Atlas directory with a valid
// atlas.sum and returns its path.
func seedAtlasPreconditionDir(c *qt.C, dir string) string {
	c.Helper()
	writeAtlasLintFile(c, dir, "20260101000000_first.sql", "CREATE TABLE users (id integer);\n")
	writeAtlasLintFile(c, dir, "20260102000000_second.sql", "CREATE TABLE posts (id integer);\n")
	hashMigrationDir(c, dir)
	return dir
}

// TestCompatCommand_MigrateLintRefusesWithoutDevURL covers stokaro/ptah#1231
// case 2.
//
//	atlas migrate lint --dir file://migrations --latest 1
//	pinned binary   exit 1   Error: required flag(s) "dev-url" not set
//	ptah-compat     exit 0   Analyzing changes … -- no diagnostics found
//
// The fixture is deliberately non-destructive. A directory whose migrations
// carry findings makes Ptah exit 1 because the lint FAILED, and the missing
// gate then looks closed; the issue records that near-miss.
func TestCompatCommand_MigrateLintRefusesWithoutDevURL(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())

	out, err := runAtlasPrecondition(c, "migrate", "lint", "--dir", "file://"+dir, "--latest", "1")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, `Error: required flag(s) "dev-url" not set`)
	c.Assert(out, qt.Not(qt.Contains), "Analyzing changes")
}

// TestCompatCommand_MigrateLintWithoutDevURLOptIn is the other half of the same
// decision: the refusal above is a compatibility default, not the removal of
// Ptah's database-free analysis. Without this test the gate would read as
// "Ptah cannot lint without a dev database", which is false.
func TestCompatCommand_MigrateLintWithoutDevURLOptIn(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")

	out, err := runAtlasPrecondition(c, "migrate", "lint", "--dir", "file://"+dir, "--latest", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "-- 1 version ok")
}

// TestCompatCommand_MigrateLintOptInIgnoresNonBooleanValues pins that the
// opt-in is a boolean, so an operator who exported it as a word does not get a
// silently looser CLI.
func TestCompatCommand_MigrateLintOptInIgnoresNonBooleanValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "false", value: "false"},
		{name: "zero", value: "0"},
		{name: "word", value: "yes please"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedAtlasPreconditionDir(c, t.TempDir())
			t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", test.value)

			out, err := runAtlasPrecondition(c, "migrate", "lint", "--dir", "file://"+dir, "--latest", "1")

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(out, qt.Contains, `Error: required flag(s) "dev-url" not set`)
		})
	}
}

// TestCompatCommand_MigrateLintRefusesWithoutChangesetSelector covers
// stokaro/ptah#1231 case 3.
//
//	atlas migrate lint --dir file://migrations --dev-url sqlite://dv?mode=memory
//	pinned binary   exit 1   Error: --latest or --git-base is required
//	ptah-compat     exit 0   Analyzing changes until version …
//
// `--latest 0` is a row because that binary treats it as unset and answers the
// same sentence, where Ptah answered `--latest must be greater than zero`.
func TestCompatCommand_MigrateLintRefusesWithoutChangesetSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector []string
	}{
		{name: "no selector", selector: nil},
		{name: "latest zero", selector: []string{"--latest", "0"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedAtlasPreconditionDir(c, t.TempDir())
			devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
			args := append([]string{"migrate", "lint", "--dir", "file://" + dir, "--dev-url", devDB}, test.selector...)

			out, err := runAtlasPrecondition(c, args...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(out, qt.Contains, "Error: --latest or --git-base is required")
			c.Assert(out, qt.Not(qt.Contains), "Analyzing changes")
		})
	}
}

// TestCompatCommand_MigrateLintAcceptsSelectorsTheBinaryAccepts is the control
// against over-correcting case 3 into a refusal of runs that binary allows.
//
// The `lint { latest = 1 }` row is the one that matters: the selector can come
// from the selected atlas.hcl env with nothing on the command line, and that
// binary exits 0 for it (measured). A gate reading the flag alone would refuse
// there, which is the forbidden direction wearing the other hat.
//
// The `--latest 99` row records that requiring a selector removes nothing: an N
// larger than the directory analyzes all of it, so "lint everything" is still
// spellable.
func TestCompatCommand_MigrateLintAcceptsSelectorsTheBinaryAccepts(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{name: "latest flag", args: []string{"--latest", "1"}},
		{name: "latest beyond the directory", args: []string{"--latest", "99"}},
		{
			name: "latest from the project env",
			project: `env "ci" {
  lint {
    latest = 1
  }
}
`,
			args: []string{"--env", "ci"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrations := seedAtlasPreconditionDir(c, filepath.Join(dir, "migrations"))
			writeAtlasLintFile(c, dir, "atlas.hcl", test.project+"\n")
			devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
			args := append([]string{"migrate", "lint", "--dir", "file://" + migrations, "--dev-url", devDB}, test.args...)

			out, err := runAtlasPrecondition(c, append(args, "--config", "file://"+filepath.Join(dir, "atlas.hcl"))...)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(out, qt.Contains, "Analyzing changes")
		})
	}
}

// TestCompatCommand_MigrateValidateRefusesReorderedChecksum covers
// stokaro/ptah#1231 case 4, the silent one.
//
//	atlas migrate validate --dir file://migrations   (entry lines swapped)
//	pinned binary   exit 1   checksum mismatch
//	ptah-compat     exit 0   (silent)
//
// Only the two entry lines move; the directory-hash line keeps the value it had
// for the original order. That is what made this invisible: the name-keyed diff
// finds every file with the hash it expects, and the hash recomputed over the
// DIRECTORY still matches the stale line, so nothing disagreed until the file
// was asked whether it agreed with itself.
//
// The absence of the `L<line>:` detail is asserted, not incidental. That binary
// prints the entry-level line only when the sum file is internally consistent
// and disagrees with the directory; here it cannot name an entry, and neither
// can Ptah.
func TestCompatCommand_MigrateValidateRefusesReorderedChecksum(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	swapAtlasSumEntryLines(c, dir)

	out, err := runAtlasPrecondition(c, "migrate", "validate", "--dir", "file://"+dir)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "You have a checksum error in your migration directory.")
	c.Assert(out, qt.Contains, "Error: checksum mismatch")
	c.Assert(out, qt.Not(qt.Contains), "L2:")
}

// TestCompatCommand_MigrateVerbsRefuseReorderedChecksum walks the verbs that
// read the directory rather than only the one the issue named. The gate is one
// verification shared by all of them, and `migrate apply` is the row that costs
// something: without it a tampered ordering executes migrations against the
// target.
func TestCompatCommand_MigrateVerbsRefuseReorderedChecksum(t *testing.T) {
	tests := []struct {
		name    string
		verb    []string
		withURL bool
	}{
		{name: "validate", verb: []string{"migrate", "validate"}},
		{name: "status", verb: []string{"migrate", "status"}, withURL: true},
		{name: "apply", verb: []string{"migrate", "apply"}, withURL: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedAtlasPreconditionDir(c, t.TempDir())
			swapAtlasSumEntryLines(c, dir)
			args := append(slices.Clone(test.verb), "--dir", "file://"+dir)
			args = append(args, atlasTargetURLArgs(t, test.withURL)...)

			out, err := runAtlasPrecondition(c, args...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(out, qt.Contains, "Error: checksum mismatch")
		})
	}
}

// atlasTargetURLArgs returns the --url pair for the verbs that register one and
// nothing for the verbs that do not. `migrate validate` reads the directory
// alone and rejects --url as an unknown flag, which would fail the row above
// for a reason that is not the checksum.
func atlasTargetURLArgs(t *testing.T, wanted bool) []string {
	t.Helper()
	urls := map[bool][]string{
		false: nil,
		true:  {"--url", "sqlite://" + filepath.Join(t.TempDir(), "target.db")},
	}
	return urls[wanted]
}

// TestCompatCommand_MigrateValidateAcceptsAnUntouchedDirectory is the control
// for the two tests above: the new question the sum file is asked must not turn
// a directory both binaries accept into a refusal.
func TestCompatCommand_MigrateValidateAcceptsAnUntouchedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())

	out, err := runAtlasPrecondition(c, "migrate", "validate", "--dir", "file://"+dir)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Equals, "")
}

// swapAtlasSumEntryLines exchanges the two entry lines of a two-migration
// atlas.sum and leaves the directory-hash line alone.
func swapAtlasSumEntryLines(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(filepath.Clean(dir), "atlas.sum")
	raw, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	c.Assert(lines, qt.HasLen, 3)
	swapped := strings.Join([]string{lines[0], lines[2], lines[1]}, "\n") + "\n"
	//nolint:gosec // the path is this test's own t.TempDir() plus a constant name
	c.Assert(os.WriteFile(path, []byte(swapped), 0o600), qt.IsNil)
}

// TestCompatCommand_SchemaApplyRefusesDryRunWithAutoApprove covers
// stokaro/ptah#1231 case 5.
//
//	atlas schema apply -u … --to … --dry-run --auto-approve
//	pinned binary   exit 1   if any flags in the group [dry-run auto-approve] are
//	                         set none of the others can be
//	ptah-compat     exit 0   prints the plan
func TestCompatCommand_SchemaApplyRefusesDryRunWithAutoApprove(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	writeAtlasLintFile(c, dir, "schema.sql", "CREATE TABLE users (id integer);\n")
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	targetDB := "sqlite://" + filepath.Join(t.TempDir(), "target.db")

	out, err := runAtlasPrecondition(c,
		"schema", "apply",
		"--url", targetDB,
		"--to", "file://"+schemaPath,
		"--dev-url", devDB,
		"--dry-run", "--auto-approve",
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains,
		"Error: if any flags in the group [dry-run auto-approve] are set none of the others can be;"+
			" [auto-approve dry-run] were all set")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
}

// TestCompatCommand_SchemaApplyAcceptsEitherFlagAlone is the control: the pair
// is refused, neither member is.
func TestCompatCommand_SchemaApplyAcceptsEitherFlagAlone(t *testing.T) {
	tests := []struct {
		name string
		flag string
	}{
		{name: "dry-run", flag: "--dry-run"},
		{name: "auto-approve", flag: "--auto-approve"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeAtlasLintFile(c, dir, "schema.sql", "CREATE TABLE users (id integer);\n")
			devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
			targetDB := "sqlite://" + filepath.Join(t.TempDir(), "target.db")

			out, err := runAtlasPrecondition(c,
				"schema", "apply",
				"--url", targetDB,
				"--to", "file://"+filepath.Join(dir, "schema.sql"),
				"--dev-url", devDB,
				test.flag,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(out, qt.Contains, "CREATE TABLE")
		})
	}
}

// TestCompatCommand_SchemaApplyApprovalGroupReadsTheCommandLine is the other
// half of the case 5 rule, and the half a first attempt got wrong.
//
// The refusal above is about what the operator typed. Implementing it with
// cmd.MarkFlagsMutuallyExclusive made it about pflag's Changed bit instead, and
// Ptah's environment binding sets Changed when it applies a PTAH_* value, so
// `--auto-approve` alone became a refusal for anyone who had exported
// PTAH_DRY_RUN — measured on 2026-08-08, exit 1 with `[auto-approve dry-run]
// were all set` for a command line carrying neither pair. The pinned community
// binary v1.3.0 exits 0 on that same command line and has no such variable, so
// the refusal was not parity; PTAH_DRY_RUN is a documented Ptah capability on
// this surface, so it was also a capability removed to buy compatibility.
//
// Each row asserts the run behaves as `--dry-run` alone does: plan printed,
// exit 0, nothing executed.
func TestCompatCommand_SchemaApplyApprovalGroupReadsTheCommandLine(t *testing.T) {
	tests := []struct {
		name     string
		envName  string
		envValue string
		flag     string
	}{
		{name: "dry run from the environment", envName: "PTAH_DRY_RUN", envValue: "1", flag: "--auto-approve"},
		{name: "dry run from the environment spelled true", envName: "PTAH_DRY_RUN", envValue: "true", flag: "--auto-approve"},
		// --auto-approve is deliberately not environment bound, so the group
		// cannot be entered from this side at all.
		{name: "auto approve is not environment bound", envName: "PTAH_AUTO_APPROVE", envValue: "1", flag: "--dry-run"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeAtlasLintFile(c, dir, "schema.sql", "CREATE TABLE users (id integer);\n")
			schemaURL := "file://" + filepath.Join(dir, "schema.sql")
			devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
			targetDB := "sqlite://" + filepath.Join(t.TempDir(), "target.db")
			t.Setenv(test.envName, test.envValue)

			out, err := runAtlasPrecondition(c,
				"schema", "apply",
				"--url", targetDB,
				"--to", schemaURL,
				"--dev-url", devDB,
				test.flag,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(out, qt.Contains, "CREATE TABLE")
			c.Assert(out, qt.Not(qt.Contains), "were all set")
			c.Assert(out, qt.Not(qt.Contains), "Auto-approval enabled")

			// Rendered is not applied. A second plan against the same target
			// still has the table to create, which is only true if the run
			// above executed nothing; an apply would leave it synced.
			second, secondErr := runAtlasPrecondition(c,
				"schema", "apply",
				"--url", targetDB,
				"--to", schemaURL,
				"--dev-url", devDB,
				"--dry-run",
			)

			c.Assert(secondErr, qt.IsNil, qt.Commentf("output:\n%s", second))
			c.Assert(second, qt.Contains, "CREATE TABLE")
			c.Assert(second, qt.Not(qt.Contains), "Schema is synced")
		})
	}
}

// TestCompatCommand_SchemaApplyAppliesWithAutoApproveAlone is the control for
// the assertion above that "no apply happened". Without it, a run that silently
// stopped applying anything at all would satisfy every row of that test.
func TestCompatCommand_SchemaApplyAppliesWithAutoApproveAlone(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, dir, "schema.sql", "CREATE TABLE users (id integer);\n")
	schemaURL := "file://" + filepath.Join(dir, "schema.sql")
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	targetDB := "sqlite://" + filepath.Join(t.TempDir(), "target.db")

	out, err := runAtlasPrecondition(c,
		"schema", "apply",
		"--url", targetDB,
		"--to", schemaURL,
		"--dev-url", devDB,
		"--auto-approve",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "Auto-approval enabled")

	second, secondErr := runAtlasPrecondition(c,
		"schema", "apply",
		"--url", targetDB,
		"--to", schemaURL,
		"--dev-url", devDB,
		"--dry-run",
	)

	c.Assert(secondErr, qt.IsNil, qt.Commentf("output:\n%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}

// TestCompatCommand_MigrateNewRefusesPathSeparatorName covers stokaro/ptah#1231
// case 6.
//
//	atlas migrate new "sub/dir_name"
//	pinned binary   exit 1   open …/<version>_sub/dir_name.sql: no such file or
//	                         directory  (nothing created)
//	ptah-compat     exit 0   creates <version>_subdir_name.sql
//
// The wording diverges deliberately: that binary's message is the raw failure
// of a write it did not expect to fail, and reproducing it would import the
// defect with the exit code. What must match is the direction and the fact that
// no file appears.
func TestCompatCommand_MigrateNewRefusesPathSeparatorName(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	before := dirEntryNames(c, dir)

	out, err := runAtlasPrecondition(c, "migrate", "new", "sub/dir_name", "--dir", "file://"+dir)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains,
		`Error: atlas migrate new "sub/dir_name": migration name must be a single file name element,`+
			` without a path separator`)
	c.Assert(dirEntryNames(c, dir), qt.DeepEquals, before)
}

// TestCompatCommand_MigrateNewAcceptsNamesTheBinaryAccepts pins the other side
// of the same rule. Each row was run against the pinned binary, which wrote a
// file and exited 0 for all three: a space, a backslash on this platform, and
// `..`. Refusing any of them here would be a new violation in the opposite
// direction.
func TestCompatCommand_MigrateNewAcceptsNamesTheBinaryAccepts(t *testing.T) {
	tests := []struct {
		name  string
		given string
	}{
		{name: "space", given: "a b"},
		{name: "backslash", given: `a\b`},
		{name: "dot dot", given: ".."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedAtlasPreconditionDir(c, t.TempDir())
			before := dirEntryNames(c, dir)

			out, err := runAtlasPrecondition(c, "migrate", "new", test.given, "--dir", "file://"+dir)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(dirEntryNames(c, dir), qt.HasLen, len(before)+1)
		})
	}
}

// TestCompatCommand_MigrateDiffRefusesPathSeparatorName is case 6's second
// branch, which the issue did not name. `migrate diff` composes a file name
// from its positional exactly as `migrate new` does, and had the same defect:
// it wrote `<version>_subname.sql` at exit 0 where the pinned binary fails the
// open at exit 1.
func TestCompatCommand_MigrateDiffRefusesPathSeparatorName(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	before := dirEntryNames(c, dir)
	desiredPath := seedSQLiteDB(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runAtlasPrecondition(c,
		"migrate", "diff", "sub/name",
		"--dir", "file://"+dir,
		"--to", "sqlite://"+desiredPath,
		"--dev-url", "sqlite://"+devPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains,
		`Error: atlas migrate diff "sub/name": migration name must be a single file name element,`+
			` without a path separator`)
	c.Assert(dirEntryNames(c, dir), qt.DeepEquals, before)
}

// TestCompatCommand_MigrateDiffIgnoresTheNameWhenNothingChanges is the
// discriminating control for the test above, and the reason the check lives
// where the file is written rather than at the top of the verb.
//
// Measured on the pinned binary: `migrate diff sub/name` against a directory
// already matching the desired state exits 0 and writes nothing -- it never
// reaches the name. A gate on the way in would refuse that run, trading one
// violation for its mirror image.
func TestCompatCommand_MigrateDiffIgnoresTheNameWhenNothingChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, dir, "20260101000000_first.sql", "CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n")
	hashMigrationDir(c, dir)
	desiredPath := seedSQLiteDB(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runAtlasPrecondition(c,
		"migrate", "diff", "sub/name",
		"--dir", "file://"+dir,
		"--to", "sqlite://"+desiredPath,
		"--dev-url", "sqlite://"+devPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestCompatCommand_MigrateDiffRefusesASecondPositional covers stokaro/ptah#1231
// case 8.
//
//	atlas migrate diff --dir … --to … one two
//	pinned binary   exit 1   accepts at most 1 arg(s), received 2
//	ptah-compat     exit 0   The migration directory is synced with the desired state
//
// The verb declared cobra.MaximumNArgs(1) all along; the shared command
// configuration assigned cmd.Args afterwards and overwrote it with nil.
func TestCompatCommand_MigrateDiffRefusesASecondPositional(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	desiredPath := seedSQLiteDB(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runAtlasPrecondition(c,
		"migrate", "diff",
		"--dir", "file://"+dir,
		"--to", "sqlite://"+desiredPath,
		"--dev-url", "sqlite://"+devPath,
		"one", "two",
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "Error: accepts at most 1 arg(s), received 2")
}

// TestCompatCommand_MigrateDiffAcceptsOnePositional is the control: one
// positional is the migration name and must keep working.
func TestCompatCommand_MigrateDiffAcceptsOnePositional(t *testing.T) {
	c := qt.New(t)
	dir := seedAtlasPreconditionDir(c, t.TempDir())
	desiredPath := seedSQLiteDB(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runAtlasPrecondition(c,
		"migrate", "diff", "add_desired_users",
		"--dir", "file://"+dir,
		"--to", "sqlite://"+desiredPath,
		"--dev-url", "sqlite://"+devPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "Created migration file:")
}

