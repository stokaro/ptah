package schematests_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

const (
	droppingPlanPrompt = "Planned schema changes:\n-- Drop \"legacy\" table\nDROP TABLE \"legacy\";\n" +
		"Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.\n"
	droppingPlanRating = "Plan rating: destructive\n" +
		"Statement 1 of 1, destructive: DROP TABLE removes the table and all rows\n" +
		"  DROP TABLE \"legacy\"\n"
)

// compatApplyStreams runs `ptah-compat schema apply` declining the prompt, with its
// standard output and standard error apart.
func compatApplyStreams(c *qt.C, policy atlascompatpolicy.Policy, args ...string) (stdout, stderr string) {
	c.Helper()
	root := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(bytes.NewBufferString("no\n"))
	root.SetArgs(append([]string{"schema", "apply"}, args...))
	c.Assert(root.Execute(), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.String(), errOut.String()))
	return out.String(), errOut.String()
}

// compatPromptDatabases seeds a database with a table a second database, the desired
// state, does not hold.
func compatPromptDatabases(c *qt.C) (target, desired string) {
	c.Helper()
	dir := c.TempDir()
	target = filepath.Join(dir, "target.db")
	desired = filepath.Join(dir, "desired.db")
	seedSQLiteSchema(c, target, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy (id INTEGER PRIMARY KEY);")
	seedSQLiteSchema(c, desired, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	return target, desired
}

// TestCompatSchemaApplyRatingStaysOffStandardOutput is the default compatibility
// surface. It keeps the rating reachable, on standard error, so that standard output
// holds the bytes strict mode holds.
func TestCompatSchemaApplyRatingStaysOffStandardOutput(t *testing.T) {
	c := qt.New(t)
	target, desired := compatPromptDatabases(c)

	stdout, stderr := compatApplyStreams(c, atlascompatpolicy.Full(), "--url", "sqlite://"+target, "--to", "sqlite://"+desired)

	c.Assert(stdout, qt.Equals, droppingPlanPrompt)
	c.Assert(stderr, qt.Equals, droppingPlanRating)
	c.Assert(atlastest.SqliteTableCount(c, target, "legacy"), qt.Equals, 1)
}

// TestCompatSchemaApplyStrictModeWritesNoRating is the policy that holds to the pinned
// binary, which rates nothing at its prompt.
func TestCompatSchemaApplyStrictModeWritesNoRating(t *testing.T) {
	c := qt.New(t)
	target, desired := compatPromptDatabases(c)

	stdout, stderr := compatApplyStreams(c, atlascompatpolicy.StrictCE(), "--url", "sqlite://"+target, "--to", "sqlite://"+desired)

	c.Assert(stdout, qt.Equals, droppingPlanPrompt)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(atlastest.SqliteTableCount(c, target, "legacy"), qt.Equals, 1)
}

// TestCompatSchemaApplySafePlanStaysQuiet is a plan with nothing to rate, on the same
// surface.
func TestCompatSchemaApplySafePlanStaysQuiet(t *testing.T) {
	c := qt.New(t)
	target, desired := compatPromptDatabases(c)
	seedSQLiteSchema(c, desired, "CREATE TABLE legacy (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);")

	stdout, stderr := compatApplyStreams(c, atlascompatpolicy.Full(), "--url", "sqlite://"+target, "--to", "sqlite://"+desired)

	c.Assert(stdout, qt.Contains, "Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.\n")
	c.Assert(stderr, qt.Equals, "")
}

// TestCompatSchemaApplyPlanFileCarriesTheRating is a saved plan, which only the default
// surface reads.
func TestCompatSchemaApplyPlanFileCarriesTheRating(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLiteSchema(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy (id INTEGER PRIMARY KEY);")
	planPath := planFileFixture(c, dir, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	contents, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Contains, "destructive")

	stdout, stderr := compatApplyStreams(c, atlascompatpolicy.Full(), "--url", "sqlite://"+dbPath, "--plan", "file://"+planPath)

	c.Assert(stdout, qt.Contains, "Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.\n")
	c.Assert(stdout, qt.Not(qt.Contains), "Plan rating")
	c.Assert(stderr, qt.Contains, "Plan rating: destructive\n")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "legacy"), qt.Equals, 1)
}

// TestCompatSchemaApplyRatesTheEditedText rates what the editor returned, since that
// is what `YES` approves.
func TestCompatSchemaApplyRatesTheEditedText(t *testing.T) {
	atlastest.InstallAppendEditor(t, "DROP TABLE users;")
	c := qt.New(t)
	target, desired := compatPromptDatabases(c)
	seedSQLiteSchema(c, desired, "CREATE TABLE legacy (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);")

	_, stderr := compatApplyStreams(c, atlascompatpolicy.Full(), "--url", "sqlite://"+target, "--to", "sqlite://"+desired, "--edit")

	c.Assert(stderr, qt.Contains, "Plan rating: destructive\n")
	c.Assert(stderr, qt.Contains, "destructive: DROP TABLE removes the table and all rows\n  DROP TABLE users\n")
}

// TestCompatSchemaApplyFormatKeepsItsOutput holds --format, whose standard output a
// template decides: the rating goes to standard error there too.
func TestCompatSchemaApplyFormatKeepsItsOutput(t *testing.T) {
	c := qt.New(t)
	target, desired := compatPromptDatabases(c)

	stdout, stderr := compatApplyStreams(c, atlascompatpolicy.Full(), "--url", "sqlite://"+target, "--to", "sqlite://"+desired, "--format", `{{ len .Changes }}`)

	c.Assert(stdout, qt.Equals, "1\nApply these schema changes? Type 'YES' to confirm: Schema apply canceled.\n")
	c.Assert(stderr, qt.Equals, droppingPlanRating)
}
