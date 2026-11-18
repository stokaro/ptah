//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A desired schema that names its tables without a schema is placed in the
// connection's current schema on a realm-scoped PostgreSQL URL, and every
// command that compares it with the target names a table it reports a fault in
// by that schema. The fixture's current schema is the role's own, not public,
// so a command that names the dialect's default schema, or none, fails here.

// schemaFault is a desired schema with one declaration fault, and the line a
// command prints for it, with %q standing for the table the fault names.
type schemaFault struct {
	name   string
	schema string
	table  string
	line   string
}

// schemaFaults are the faults the commands report: an index fault, which the
// commands find before they compare, and a fault the comparison finds.
var schemaFaults = []schemaFault{
	{
		name:   "index on an undeclared column",
		schema: "CREATE TABLE t (id bigint PRIMARY KEY);\nCREATE INDEX i ON t (missing);\n",
		table:  "t",
		line:   `postgres: index "i": names column "missing", which table %q does not declare`,
	},
	{
		name: "foreign key to a column that is not unique",
		schema: "CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL);\n" +
			"CREATE TABLE child (id bigint PRIMARY KEY, code text REFERENCES parent (code));\n",
		table: "parent",
		line:  `postgres requires referenced columns code on table %q to be declared unique`,
	},
}

// soundSchema is the schema of [schemaFaults] with both faults corrected.
const soundSchema = "CREATE TABLE t (id bigint PRIMARY KEY);\nCREATE INDEX i ON t (id);\n" +
	"CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL UNIQUE);\n" +
	"CREATE TABLE child (id bigint PRIMARY KEY, code text REFERENCES parent (code));\n"

// targetCommand is a command that reads the desired schema against the
// target: the verb, the flags after the target's URL, and the exit status of a
// run on [soundSchema] against an empty target.
type targetCommand struct {
	verb      string
	extra     []string
	soundExit int
}

// targetCommands write nothing, so the rows of one test share one target.
var targetCommands = []targetCommand{
	{verb: "drift", soundExit: 1},
	{verb: "plan", extra: []string{"--dry-run"}, soundExit: 0},
	{verb: "apply", extra: []string{"--dry-run"}, soundExit: 0},
	{verb: "compare", soundExit: 0},
}

// run runs the command against targetURL with schema as schema.sql.
func (cmd targetCommand) run(c *qt.C, targetURL, schema string) clirun.Result {
	c.Helper()
	args := append([]string{"schema", cmd.verb, "--schema-file", "schema.sql", "--db-url", targetURL}, cmd.extra...)
	return clirun.Run(c, clirun.Ptah, clirun.Options{
		Dir: realmCurrentSchemaRoot(c, schema),
		Env: environmentWithoutPtahVariables(),
	}, args...)
}

// TestSchemaCommandsNameAFaultTableByTheCurrentSchema_FailurePathE2E runs each
// command on each fault and expects the table named as schema.table, the
// schema read back from the target with current_schema(). Without the
// placement ahead of the index check, plan and apply name the index fault's
// table t, and compare plans the index instead of refusing it.
func TestSchemaCommandsNameAFaultTableByTheCurrentSchema_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c)
	current := realmCurrentSchemaNames(c, fixture.target, "SELECT current_schema()")
	c.Assert(current, qt.HasLen, 1)

	for _, fault := range schemaFaults {
		t.Run(fault.name, func(t *testing.T) {
			for _, command := range targetCommands {
				t.Run(command.verb, func(t *testing.T) {
					c := qt.New(t)
					result := command.run(c, fixture.targetURL, fault.schema)
					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
					c.Assert(result.ExitCode, qt.Equals, 2, comment)
					c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(fault.line, current[0]+"."+fault.table), comment)
				})
			}
		})
	}
}

// TestSchemaCommandsReadTheCorrectedSchema_HappyPathE2E runs each command on
// [soundSchema], so the refusals above are the faults' doing and not the
// fixture's. Drift exits 1 because the target holds none of the tables.
func TestSchemaCommandsReadTheCorrectedSchema_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c)

	for _, command := range targetCommands {
		t.Run(command.verb, func(t *testing.T) {
			c := qt.New(t)
			result := command.run(c, fixture.targetURL, soundSchema)
			c.Assert(result.ExitCode, qt.Equals, command.soundExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		})
	}
}

// TestSchemaValidateNamesAFaultTableAsTheSourceWritesIt_FailurePathE2E runs
// `schema validate`, which connects to no database and so cannot know which
// schema a target would place a bare name in. It names the table as the source
// writes it.
func TestSchemaValidateNamesAFaultTableAsTheSourceWritesIt_FailurePathE2E(t *testing.T) {
	for _, fault := range schemaFaults {
		t.Run(fault.name, func(t *testing.T) {
			c := qt.New(t)
			result := clirun.Run(c, clirun.Ptah, clirun.Options{
				Dir: realmCurrentSchemaRoot(c, fault.schema),
				Env: environmentWithoutPtahVariables(),
			}, "schema", "validate", "--schema-file", "schema.sql", "--dialect", "postgres")
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 1, comment)
			c.Assert(result.Stdout, qt.Contains, fmt.Sprintf(fault.line, fault.table), comment)
		})
	}
}

// TestSchemaValidateAcceptsTheCorrectedSchema_HappyPathE2E validates
// [soundSchema], which reports nothing.
func TestSchemaValidateAcceptsTheCorrectedSchema_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{
		Dir: realmCurrentSchemaRoot(c, soundSchema),
		Env: environmentWithoutPtahVariables(),
	}, "schema", "validate", "--schema-file", "schema.sql", "--dialect", "postgres")
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stdout, qt.Equals, "")
}
