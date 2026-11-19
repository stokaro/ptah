package migratetests_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
)

// compatLeavingOutSchema declares a table option and an index type SQLite has
// no clause for, so every statement SQLite writes for them leaves both out.
const compatLeavingOutSchema = "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;\n" +
	"CREATE INDEX t_name ON t USING HASH (name);\n"

// compatPlanCommands are the compatibility commands that render a plan. Each
// runs in a directory holding schema.sql, an empty migrations directory and
// the SQLite database app.db.
var compatPlanCommands = []struct {
	name string
	args []string
}{
	{name: "migrate diff", args: []string{"migrate", "diff", "first", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory"}},
	{name: "schema apply", args: []string{"schema", "apply", "--url", "sqlite://app.db", "--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory", "--auto-approve"}},
	{name: "schema diff", args: []string{"schema", "diff", "--from", "sqlite://app.db", "--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory"}},
}

// enterOmissionDirectory makes a fresh directory holding the schema file and
// an empty migrations directory the working directory of the test.
func enterOmissionDirectory(c *qt.C, t *testing.T) {
	c.Helper()
	t.Chdir(c.TempDir())
	c.Assert(os.WriteFile("schema.sql", []byte(compatLeavingOutSchema), 0o600), qt.IsNil)
	c.Assert(os.Mkdir("migrations", 0o700), qt.IsNil)
}

// runCompatRun runs args on the compatibility tree as one run and returns
// its exit code and its two streams.
func runCompatRun(c *qt.C, args ...string) (code int, stdout, stderr string) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	code = root.RunContext(c.Context(), cmd, args...)
	return code, out.String(), errOut.String()
}

// compatSkippedWarnings are the warnings in stderr about a declaration a
// render went on without.
func compatSkippedWarnings(stderr string) []string {
	var warnings []string
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "warning: ") && strings.Contains(line, " was skipped (") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// TestCompatPlanCommands_WarnAboutWhatTheirStatementsLeaveOut runs each
// compatibility command that renders a plan against an empty SQLite database
// or migration directory. Each warns on stderr, once, about the table option
// and the index type its CREATE statements leave out, in the words the
// native commands use.
func TestCompatPlanCommands_WarnAboutWhatTheirStatementsLeaveOut(t *testing.T) {
	for _, command := range compatPlanCommands {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			enterOmissionDirectory(c, t)

			code, stdout, stderr := runCompatRun(c, command.args...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(compatSkippedWarnings(stderr), qt.DeepEquals, []string{
				"warning: sqlite: index t_name: index type=HASH was skipped (unsupported)",
				"warning: sqlite: table t: table option ENGINE=InnoDB was skipped (unsupported)",
			})
			c.Assert(stdout, qt.Not(qt.Contains), "warning:")
		})
	}
}

// TestCompatPlanCommands_WarnAboutNothingOnceSynced is the control. Once the
// database holds the schema and the directory holds a file for it, a plan
// holds no statement, so no command renders what SQLite cannot hold and none
// warns about it.
func TestCompatPlanCommands_WarnAboutNothingOnceSynced(t *testing.T) {
	for _, command := range compatPlanCommands {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			enterOmissionDirectory(c, t)
			applied, _, applyErr := runCompatRun(c, compatPlanCommands[1].args...)
			c.Assert(applied, qt.Equals, 0, qt.Commentf("%s", applyErr))
			diffed, _, diffErr := runCompatRun(c, compatPlanCommands[0].args...)
			c.Assert(diffed, qt.Equals, 0, qt.Commentf("%s", diffErr))

			code, stdout, stderr := runCompatRun(c, command.args...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(compatSkippedWarnings(stderr), qt.IsNil)
		})
	}
}

// TestCompatMigrateDiff_FileCarriesNoWarning reads the file `migrate diff`
// writes. The warnings stay on stderr: the file is the one the community
// binary writes, and atlas.sum hashes its bytes.
func TestCompatMigrateDiff_FileCarriesNoWarning(t *testing.T) {
	c := qt.New(t)
	enterOmissionDirectory(c, t)

	code, stdout, stderr := runCompatRun(c, compatPlanCommands[0].args...)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	files, globErr := filepath.Glob(filepath.Join("migrations", "*_first.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	written, readErr := os.ReadFile(files[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(written), qt.Not(qt.Contains), "was skipped")
	c.Assert(string(written), qt.Contains, "CREATE INDEX \"t_name\" ON \"t\" (\"name\");\n")
}
