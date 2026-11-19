package root_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/root"
)

// leavingOutSchema declares a table option and an index type SQLite has no
// clause for, so every statement SQLite writes for them leaves both out.
const leavingOutSchema = "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;\n" +
	"CREATE INDEX t_name ON t USING HASH (name);\n"

// leftOutWarnings are the warnings a plan of leavingOutSchema raises on SQLite.
var leftOutWarnings = []string{
	"warning: sqlite: index t_name: index type=HASH was skipped (unsupported)",
	"warning: sqlite: table t: table option ENGINE=InnoDB was skipped (unsupported)",
}

// omissionFixture is a SQLite database path, the schema file, and a directory
// for migration files, each in a directory of its own.
type omissionFixture struct {
	dbURL, schema, migrations string
}

func newOmissionFixture(c *qt.C) omissionFixture {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(leavingOutSchema), 0o600), qt.IsNil)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	return omissionFixture{dbURL: "sqlite://" + filepath.Join(dir, "app.db"), schema: schema, migrations: migrations}
}

// args is template with DB, SCHEMA and DIR replaced by the fixture's paths.
func (f omissionFixture) args(template []string) []string {
	replacer := strings.NewReplacer("DB", f.dbURL, "SCHEMA", f.schema, "DIR", f.migrations)
	args := make([]string, 0, len(template))
	for _, arg := range template {
		args = append(args, replacer.Replace(arg))
	}
	return args
}

// runStreams runs args on the native tree as one run and returns its exit
// code and its two streams.
func runStreams(c *qt.C, args ...string) (code int, stdout, stderr string) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	code = root.RunContext(c.Context(), cmd, args...)
	return code, out.String(), errOut.String()
}

// skippedWarnings are the warnings in stderr about a declaration a render
// went on without.
func skippedWarnings(stderr string) []string {
	var warnings []string
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "warning: ") && strings.Contains(line, " was skipped (") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// planCommands are the native commands that render a plan, as templates.
var planCommands = []struct {
	name string
	args []string
}{
	{name: "schema apply --dry-run", args: []string{"schema", "apply", "--db-url", "DB", "--schema-file", "SCHEMA", "--dry-run"}},
	{name: "schema apply", args: []string{"schema", "apply", "--db-url", "DB", "--schema-file", "SCHEMA", "--auto-approve"}},
	{name: "schema plan", args: []string{"schema", "plan", "--db-url", "DB", "--schema-file", "SCHEMA", "--output", "DIR/t.plan.json"}},
	{name: "schema compare", args: []string{"schema", "compare", "--db-url", "DB", "--schema-file", "SCHEMA"}},
	{name: "schema diff", args: []string{"schema", "diff", "--from", "DB", "--to", "file://SCHEMA", "--dev-url", "sqlite://dev?mode=memory"}},
	{name: "migrations plan", args: []string{"migrations", "plan", "--db-url", "DB", "--schema-file", "SCHEMA"}},
	{name: "migrations generate", args: []string{"migrations", "generate", "--db-url", "DB", "--schema-file", "SCHEMA", "--migrations-dir", "DIR"}},
}

// TestPlanCommands_WarnAboutWhatTheirStatementsLeaveOut runs every native
// command that renders a plan against an empty SQLite database. Each warns,
// once, about the table option and the index type the CREATE statements leave
// out, on stderr, as `schema render` does, and writes no warning on stdout.
func TestPlanCommands_WarnAboutWhatTheirStatementsLeaveOut(t *testing.T) {
	for _, command := range planCommands {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			f := newOmissionFixture(c)

			code, stdout, stderr := runStreams(c, f.args(command.args)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(skippedWarnings(stderr), qt.DeepEquals, leftOutWarnings)
			c.Assert(stdout, qt.Not(qt.Contains), "warning:")
		})
	}
}

// TestPlanCommands_WarnAboutNothingOnceTheDatabaseIsSynced is the control.
// Once the schema is applied, a plan holds no statement, so no command
// renders the table option or the index type SQLite cannot hold, and none
// warns about them.
func TestPlanCommands_WarnAboutNothingOnceTheDatabaseIsSynced(t *testing.T) {
	for _, command := range planCommands {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			f := newOmissionFixture(c)
			applied, _, applyErr := runStreams(c, "schema", "apply", "--db-url", f.dbURL, "--schema-file", f.schema, "--auto-approve")
			c.Assert(applied, qt.Equals, 0, qt.Commentf("%s", applyErr))

			code, stdout, stderr := runStreams(c, f.args(command.args)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(skippedWarnings(stderr), qt.IsNil)
		})
	}
}

// TestMigrationsGenerate_FileCarriesWhatItsStatementsLeaveOut reads the up
// file `migrations generate` writes: the warnings open its statements as
// comments, so a reviewer of the file reads them too. The down file drops
// what the up file creates and leaves nothing out.
func TestMigrationsGenerate_FileCarriesWhatItsStatementsLeaveOut(t *testing.T) {
	c := qt.New(t)
	f := newOmissionFixture(c)

	code, stdout, stderr := runStreams(c, "migrations", "generate", "--db-url", f.dbURL, "--schema-file", f.schema,
		"--migrations-dir", f.migrations, "--name", "first")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	up, upErr := filepath.Glob(filepath.Join(f.migrations, "*_first.up.sql"))
	c.Assert(upErr, qt.IsNil)
	c.Assert(up, qt.HasLen, 1)
	upSQL, readErr := os.ReadFile(up[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "-- Direction: UP\n\n"+
		"-- sqlite: index t_name: index type=HASH was skipped (unsupported)\n"+
		"-- sqlite: table t: table option ENGINE=InnoDB was skipped (unsupported)\n\n"+
		"CREATE TABLE \"t\" (")
	down, downErr := filepath.Glob(filepath.Join(f.migrations, "*_first.down.sql"))
	c.Assert(downErr, qt.IsNil)
	c.Assert(down, qt.HasLen, 1)
	downSQL, readDownErr := os.ReadFile(down[0])
	c.Assert(readDownErr, qt.IsNil)
	c.Assert(string(downSQL), qt.Not(qt.Contains), "was skipped")
}
