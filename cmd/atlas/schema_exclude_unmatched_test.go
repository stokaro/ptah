package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// writeUnmatchedExcludeSchema writes the one-table desired schema every case
// below applies, and returns its path.
func writeUnmatchedExcludeSchema(tb testing.TB, dir string) string {
	c := qt.New(tb)
	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE exclude_keep (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	return path
}

// TestSchemaDiffWarnsWhenAnExcludeSelectorMatchedNothing is the read-only half
// of the fail-open decision in stokaro/ptah#933. The pinned community binary
// v1.3.0 exits 0 and says nothing for `--exclude nosuchobject`; matching that
// silence is the one thing Ptah declines to copy, because a selector that
// protected nothing is indistinguishable from one that worked.
//
// Diff previews rather than executes, so it keeps its exit status and reports
// out of band — the same split #1113 recorded for --include.
//
// Red before the report plumbing: stderr is empty and the assertion on the
// notice fails.
func TestSchemaDiffWarnsWhenAnExcludeSelectorMatchedNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	to := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--exclude", "nosuch_object",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Contains, `the --exclude selection matched no objects: "nosuch_object"`)
	c.Assert(out.String(), qt.Contains, "exclude_keep")
}

// TestSchemaDiffStaysQuietWhenTheExcludeSelectorMatched is the control that
// makes the test above a measurement rather than an unconditional message.
func TestSchemaDiffStaysQuietWhenTheExcludeSelectorMatched(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	to := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--exclude", "exclude_keep",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
}

// TestSchemaApplyRefusesAnExcludeSelectorThatMatchedNothing is the executing
// half. Apply is the verb that carries the plan out, so an --exclude that
// protected nothing refuses there instead of warning: the user wrote it to keep
// an object out of the plan, and a selector that named nothing leaves the plan
// free to change it.
//
// Red before the refusal: the command exits 0 and applies the plan.
func TestSchemaApplyRefusesAnExcludeSelectorThatMatchedNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dry-run",
		"--exclude", "nosuch_object",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`the --exclude selection matched no objects: "nosuch_object"; schema apply refuses a selection that protects nothing.*`)
	c.Assert(err, qt.ErrorMatches, `.*PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1.*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "exclude_keep")
}

// TestSchemaApplyAcceptsAColumnSelectorUnderAnExcludedTable is the CLI form of
// the column gap the refusal exposed.
//
// `--exclude exclude_keep --exclude exclude_keep.id` names two objects that both
// exist, and the pinned community binary v1.3.0 exits 0 for it. Ptah exited 1
// with `the --exclude selection matched no objects: "exclude_keep.id"`, because
// filterTables `continue`s on a table match and never reaches filterColumns, so
// the column pattern was never asked. A refusal that is right about the general
// case and wrong about real objects is worse than the silence it replaced.
//
// Red without the fix: `cmd.Execute()` returns that refusal.
func TestSchemaApplyAcceptsAColumnSelectorUnderAnExcludedTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dry-run",
		"--exclude", "exclude_keep",
		"--exclude", "exclude_keep.id",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Not(qt.Contains), "matched no objects")
}

// TestSchemaApplyStillRefusesAColumnOfAnExcludedTableThatDoesNotExist is the
// inverse mutant of the test above: asking the column patterns has to stay a
// name test. A fix that marked every pattern satisfied the moment a table was
// excluded would pass that test and turn this one from red to green-by-accident.
func TestSchemaApplyStillRefusesAColumnOfAnExcludedTableThatDoesNotExist(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dry-run",
		"--exclude", "exclude_keep",
		"--exclude", "exclude_keep.nosuch_column",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`the --exclude selection matched no objects: "exclude_keep.nosuch_column";.*`)
}

// TestSchemaApplyKeepsThePermissiveBehaviorBehindTheOptIn is the capability
// half of the same decision: refusing is the safe default, and a shared exclude
// list reused across environments can legitimately name an object one of them
// does not have. Compatibility never removes a capability, so the permissive
// behavior stays reachable on this same surface — through an environment
// variable, never a new flag, because the conformance cli-surface tier asserts
// flag parity with the pinned community binary.
func TestSchemaApplyKeepsThePermissiveBehaviorBehindTheOptIn(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasfilter.AllowUnmatchedExcludeEnvVar, "1")
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := writeUnmatchedExcludeSchema(c.TB, dir)
	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dry-run",
		"--exclude", "nosuch_object",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Contains, `the --exclude selection matched no objects: "nosuch_object"`)
	c.Assert(out.String(), qt.Contains, "exclude_keep")
}

// TestSchemaInspectWarnsWhenAnExcludeSelectorMatchedNothing covers the third
// verb the issue's reproductions use. Inspection is read-only, so it keeps
// exit 0 and says on stderr that the selector protected nothing.
func TestSchemaInspectWarnsWhenAnExcludeSelectorMatchedNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect.db")
	schemaPath := writeUnmatchedExcludeSchema(c.TB, dir)
	apply := atlas.NewCompatCommand("atlas")
	apply.SetOut(&bytes.Buffer{})
	apply.SetErr(&bytes.Buffer{})
	apply.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--auto-approve",
	})
	c.Assert(apply.Execute(), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--exclude", "nosuch_object",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Contains, `the --exclude selection matched no objects: "nosuch_object"`)
	c.Assert(out.String(), qt.Contains, "exclude_keep")
}
