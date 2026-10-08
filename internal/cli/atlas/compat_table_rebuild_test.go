package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/envbool/envbooltest"
)

// tableRebuildVar is the variable this surface reads where the native commands
// take --allow-table-rebuild.
const tableRebuildVar = "PTAH_ALLOW_TABLE_REBUILD"

// writeRebuildDiffFiles writes two SQL documents for one YDB table that differ
// by a column type, a change YDB makes only by rebuilding the table, and
// returns them as file:// URLs.
func writeRebuildDiffFiles(c *qt.C) (from, to string) {
	c.Helper()
	dir := c.TempDir()
	document := func(columnType string) []byte {
		return []byte("CREATE TABLE rb (id Int64 NOT NULL, v " + columnType + ", PRIMARY KEY (id));")
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "from.sql"), document("Int32"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "to.sql"), document("Int64"), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(filepath.Join(dir, "from.sql")),
		"file://" + filepath.ToSlash(filepath.Join(dir, "to.sql"))
}

// rebuildDiffArgs diffs the two documents on YDB. A diff between two files
// connects to nothing; the dev URL only names the dialect, so one the YDB
// connector would refuse is never opened.
func rebuildDiffArgs(c *qt.C) []string {
	c.Helper()
	from, to := writeRebuildDiffFiles(c)
	return []string{"schema", "diff", "--from", from, "--to", to, "--dev-url", ydbConnectorRefusedURL}
}

// This surface takes no flag the pinned binary lacks, so the refusal of a
// change only a rebuild can make names the variable, where the native surface
// names --allow-table-rebuild.
func TestCompatSchemaDiffNamesTheRebuildVariableOnYDB(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(tableRebuildVar)(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), rebuildDiffArgs(c)...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: generate schema diff SQL: changing the type of column \"v\" of table \"rb\" "+
		"(Int32 -> Int64), which requires target capability alter_column_type, unavailable on this ydb target; YDB "+
		"makes it by rebuilding the table, which Ptah plans when asked with PTAH_ALLOW_TABLE_REBUILD=1\n")
}

// The variable reaches the plan: set, the same diff plans the rebuild, and a
// valid false keeps the refusal.
func TestCompatSchemaDiffPlansARebuildWhenAsked(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(tableRebuildVar, "1")(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), rebuildDiffArgs(c)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, "CREATE TABLE `__ptah_rebuild_rb` (\n")
	c.Assert(stdout, qt.Contains, "ALTER TABLE `rb` RENAME TO `__ptah_replaced_rb`;\n"+
		"ALTER TABLE `__ptah_rebuild_rb` RENAME TO `rb`;\n"+
		"DROP TABLE `__ptah_replaced_rb`;\n")
}

// The control: false is a value, and it asks for nothing.
func TestCompatSchemaDiffKeepsTheRefusalWhenTheVariableIsFalse(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(tableRebuildVar, "false")(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), rebuildDiffArgs(c)...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "which Ptah plans when asked with PTAH_ALLOW_TABLE_REBUILD=1\n")
}

// Each verb that plans resolves the variable before its first early return, so
// a malformed value fails the run before anything else is looked at: each row
// would otherwise fail on a missing flag, as TestCompatPlanningVerbsFailOnTheirOwnFirst
// shows.
func TestCompatRefusesAMalformedRebuildVariableBeforeAnyWork(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema apply", args: []string{"schema", "apply"}},
		{name: "schema diff", args: []string{"schema", "diff"}},
		{name: "schema plan new", args: []string{"schema", "plan", "new"}},
		{name: "migrate diff", args: []string{"migrate", "diff"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(tableRebuildVar, "maybe")(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: invalid boolean value \"maybe\" for PTAH_ALLOW_TABLE_REBUILD\n")
		})
	}
}

// The control for the rows above: with the variable unset each verb fails on
// its own first check.
func TestCompatPlanningVerbsFailOnTheirOwnFirst(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "schema apply", args: []string{"schema", "apply"}, want: "Error: required flag(s) \"url\" not set\n"},
		{name: "schema diff", args: []string{"schema", "diff"}, want: "Error: --from is required\n"},
		{name: "schema plan new", args: []string{"schema", "plan", "new"}, want: "Error: --from is required\n"},
		{name: "migrate diff", args: []string{"migrate", "diff"}, want: "Error: --to is required\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(tableRebuildVar)(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// A verb that plans nothing does not read the variable, so a malformed value
// leaves it to fail on its own first check.
func TestCompatSchemaInspectDoesNotReadTheRebuildVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(tableRebuildVar, "maybe")(t)

	stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: required flag(s) \"url\" not set\n")
}

// The pinned community binary plans no table rebuild, so the strict profile
// refuses the variable as it refuses every gated PTAH_* toggle, at the process
// boundary and before any command runs; a valid false asks for nothing and is
// kept.
func TestStrictCompatRefusesTheRebuildVariable(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "asked for", value: "1",
			wantErr: "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ALLOW_TABLE_REBUILD"},
		{name: "malformed", value: "maybe",
			wantErr: `invalid boolean value "maybe" for PTAH_ALLOW_TABLE_REBUILD`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
			envbooltest.Set(tableRebuildVar, test.value)(t)

			policy, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(policy.IsStrictCE(), qt.IsFalse)
		})
	}
}

// The control for the strict rows: false is kept, and strict mode is selected.
func TestStrictCompatKeepsAFalseRebuildVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
	envbooltest.Set(tableRebuildVar, "0")(t)

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.IsStrictCE(), qt.IsTrue)
}
