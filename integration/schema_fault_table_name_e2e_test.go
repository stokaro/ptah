//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A desired schema that names its tables without a schema is placed in a
// connection's current schema on a realm-scoped PostgreSQL URL, and every
// command that compares it names a table it reports a fault in by that schema.
// The fixture's target resolves names to the role's own schema, not public, so
// a command that names the dialect's default schema, or none, fails here.

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
		line: `postgres requires referenced columns code on table %q to be declared unique: ` +
			`a primary key, UNIQUE constraint or unique index must key exactly those columns`,
	},
}

// soundSchema is the schema of [schemaFaults] with both faults corrected.
const soundSchema = "CREATE TABLE t (id bigint PRIMARY KEY);\nCREATE INDEX i ON t (id);\n" +
	"CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL UNIQUE);\n" +
	"CREATE TABLE child (id bigint PRIMARY KEY, code text REFERENCES parent (code));\n"

// excludedFaultSchema holds the index fault on t beside a sound table u, for
// a run that excludes t.
const excludedFaultSchema = "CREATE TABLE t (id bigint PRIMARY KEY);\nCREATE INDEX i ON t (missing);\n" +
	"CREATE TABLE u (id bigint PRIMARY KEY);\n"

// targetCommand is a command that compares the desired schema in schema.sql.
// In args, {target} stands for the target's URL, {dev} for the dev database's,
// and {schema} for the current schema of the database the command places names
// on, which placedOn names: the target, or the dev database for a diff of two
// documents. soundExit is the exit status of a run on [soundSchema]; drift
// exits 1 because the target holds none of the tables.
type targetCommand struct {
	name      string
	args      []string
	placedOn  string
	soundExit int
}

// targetCommands write nothing to the target, so the rows of one test share
// one fixture.
var targetCommands = []targetCommand{
	{
		name:     "schema drift",
		args:     []string{"schema", "drift", "--schema-file", "schema.sql", "--db-url", "{target}"},
		placedOn: "target", soundExit: 1,
	},
	{
		name:     "schema plan",
		args:     []string{"schema", "plan", "--schema-file", "schema.sql", "--db-url", "{target}", "--dry-run"},
		placedOn: "target",
	},
	{
		name:     "schema apply",
		args:     []string{"schema", "apply", "--schema-file", "schema.sql", "--db-url", "{target}", "--dry-run"},
		placedOn: "target",
	},
	{
		name:     "schema compare",
		args:     []string{"schema", "compare", "--schema-file", "schema.sql", "--db-url", "{target}"},
		placedOn: "target",
	},
	{
		name:     "schema diff of two documents",
		args:     []string{"schema", "diff", "--from", "file://empty.sql", "--to", "file://schema.sql", "--dev-url", "{dev}"},
		placedOn: "dev",
	},
}

// fromDocumentDiff is `schema diff` with the schema as its --from document. It
// checks that document as it checks a --to one, for both faults, and names the
// table by the dev database's current schema.
var fromDocumentDiff = targetCommand{
	name:     "schema diff of two documents, the schema as --from",
	args:     []string{"schema", "diff", "--from", "file://schema.sql", "--to", "file://empty.sql", "--dev-url", "{dev}"},
	placedOn: "dev",
}

// faultFixture is the realm fixture with the current schema of its target and
// of its dev database, read from each with current_schema().
type faultFixture struct {
	realmCurrentSchemaFixture
	current map[string]string
}

// newFaultFixture creates the realm fixture and reads both current schemas.
func newFaultFixture(c *qt.C) faultFixture {
	c.Helper()
	fixture := newRealmCurrentSchemaFixture(c)
	dev, err := sql.Open("pgx", fixture.devURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = dev.Close() }()
	target := realmCurrentSchemaNames(c, fixture.target, "SELECT current_schema()")
	c.Assert(target, qt.HasLen, 1)
	devSchema := realmCurrentSchemaNames(c, dev, "SELECT current_schema()")
	c.Assert(devSchema, qt.HasLen, 1)
	return faultFixture{
		realmCurrentSchemaFixture: fixture,
		current:                   map[string]string{"target": target[0], "dev": devSchema[0]},
	}
}

// run runs cmd with schema as schema.sql and an empty empty.sql beside it,
// extra added to the command line.
func (f faultFixture) run(c *qt.C, cmd targetCommand, schema string, extra ...string) clirun.Result {
	c.Helper()
	root := realmCurrentSchemaRoot(c, schema)
	c.Assert(os.WriteFile(filepath.Join(root, "empty.sql"), nil, 0o600), qt.IsNil)
	placeholders := strings.NewReplacer("{target}", f.targetURL, "{dev}", f.devURL, "{schema}", f.current[cmd.placedOn])
	args := make([]string, 0, len(cmd.args)+len(extra))
	for _, arg := range slices.Concat(cmd.args, extra) {
		args = append(args, placeholders.Replace(arg))
	}
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}, args...)
}

// TestSchemaCommandsNameAFaultTableByTheCurrentSchema_FailurePathE2E runs each
// command on each fault and expects the table named as schema.table, the
// schema read back with current_schema() from the database the command places
// names on, on an error line that starts with the native prefix. Without the
// placement ahead of the index check, plan and apply name the index fault's
// table t; without the check, compare and diff plan the index instead of
// refusing it; and drift prints its own prefix, Error.
func TestSchemaCommandsNameAFaultTableByTheCurrentSchema_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)

	for _, fault := range schemaFaults {
		t.Run(fault.name, func(t *testing.T) {
			for _, cmd := range targetCommands {
				t.Run(cmd.name, func(t *testing.T) {
					c := qt.New(t)
					result := fixture.run(c, cmd, fault.schema)
					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
					c.Assert(result.ExitCode, qt.Equals, 2, comment)
					c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(fault.line, fixture.current[cmd.placedOn]+"."+fault.table), comment)
					c.Assert(result.Stderr, qt.Matches, `(?s)(.*\n)?error: [^\n]*\n`, comment)
				})
			}
		})
	}
}

// TestSchemaDiffNamesAFaultOfItsFromDocument_FailurePathE2E runs
// [fromDocumentDiff] on each fault, and on the index fault with t excluded, and
// expects the line of a --to document, the table named by the dev database's
// current schema. Without the check of the --from side, the diff plans the
// tables' removal and exits 0: the index check covers the index rows, and the
// target validation the foreign-key row.
func TestSchemaDiffNamesAFaultOfItsFromDocument_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)

	for _, test := range []struct {
		name   string
		schema string
		extra  []string
		fault  schemaFault
		side   string
	}{
		{name: schemaFaults[0].name, schema: schemaFaults[0].schema, fault: schemaFaults[0]},
		{name: schemaFaults[1].name, schema: schemaFaults[1].schema, fault: schemaFaults[1], side: fromSideRefusal},
		{
			name: "index on an undeclared column of an excluded table", schema: excludedFaultSchema,
			extra: []string{"--exclude", "{schema}.t"}, fault: schemaFaults[0],
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := fmt.Sprintf(test.fault.line, fixture.current[fromDocumentDiff.placedOn]+"."+test.fault.table)
			result := fixture.run(c, fromDocumentDiff, test.schema, test.extra...)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 2, comment)
			c.Assert(result.Stderr, qt.Equals, "error: "+test.side+want+"\n", comment)
		})
	}
}

// fromSideRefusal is the prefix of a --from document's foreign-key refusal,
// which names the side, as a --from that does not load is named. A --to
// document's refusal carries none.
const fromSideRefusal = "load --from schema: "

// unkeyedReferenceSound is the foreign-key fault of [schemaFaults] with the
// foreign key left out.
const unkeyedReferenceSound = "CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL);\n" +
	"CREATE TABLE child (id bigint PRIMARY KEY, code text);\n"

// referenceFaultBesideSetNull is the foreign-key fault of [schemaFaults] with
// another fault declared ahead of it: a SET NULL action on a NOT NULL column.
// Ptah's target validation refuses that action and PostgreSQL creates it. The
// validation stops at its first refusal, so a --from check that runs it whole
// reports nothing about the foreign key, which PostgreSQL refuses with 42830.
const referenceFaultBesideSetNull = "CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL);\n" +
	"CREATE TABLE other (id bigint PRIMARY KEY, parent_id bigint NOT NULL REFERENCES parent (id) ON DELETE SET NULL);\n" +
	"CREATE TABLE child (id bigint PRIMARY KEY, code text REFERENCES parent (code));\n"

// runDocumentDiff runs `schema diff --from file://from.sql --to file://to.sql`
// on the fixture's dev database, extra added to the command line with
// {schema} standing for the dev database's current schema.
func (f faultFixture) runDocumentDiff(c *qt.C, from, to string, extra ...string) clirun.Result {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "from.sql"), []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "to.sql"), []byte(to), 0o600), qt.IsNil)
	args := []string{"schema", "diff", "--from", "file://from.sql", "--to", "file://to.sql", "--dev-url", f.devURL}
	for _, arg := range extra {
		args = append(args, strings.ReplaceAll(arg, "{schema}", f.current["dev"]))
	}
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}, args...)
}

// TestSchemaDiffRefusesAReferenceFaultOfEitherDocument_FailurePathE2E puts the
// foreign-key fault on each side of a diff with the sound schema. PostgreSQL
// refuses such a key when it creates the table, so the --from document is
// held to the rule as the --to one is, and its line names the side. The last
// row declares a fault of another rule first: the --from document is held to
// the referenced-key rule alone, so that fault does not hide the key.
func TestSchemaDiffRefusesAReferenceFaultOfEitherDocument_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)
	line := fmt.Sprintf(schemaFaults[1].line, fixture.current["dev"]+".parent")

	for _, test := range []struct {
		name     string
		from, to string
		want     string
	}{
		{name: "faulty --from", from: schemaFaults[1].schema, to: unkeyedReferenceSound, want: "error: " + fromSideRefusal + line + "\n"},
		{name: "faulty --to", from: unkeyedReferenceSound, to: schemaFaults[1].schema, want: "error: " + line + "\n"},
		{name: "faulty --from beside a SET NULL fault", from: referenceFaultBesideSetNull, to: unkeyedReferenceSound, want: "error: " + fromSideRefusal + line + "\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := fixture.runDocumentDiff(c, test.from, test.to)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 2, comment)
			c.Assert(result.Stderr, qt.Equals, test.want, comment)
			c.Assert(result.Stdout, qt.Equals, "", comment)
		})
	}
}

// referenceParentAlone is the parent table of [schemaFaults]'s foreign-key
// fault, without child.
const referenceParentAlone = "CREATE TABLE parent (id bigint PRIMARY KEY, code text NOT NULL);\n"

// TestSchemaDiffChecksTheWholeDocumentOfEitherSide_FailurePathE2E puts the
// foreign-key fault on each side and excludes one of its two tables. The
// pinned community binary creates each document whole on its dev database, so
// it exits 1 with the server's 42830 in every row, whatever --exclude selects.
// The rule reads the placed document, not the selection, so each row is
// refused: checked on the selection, the parent rows plan a child with no key
// or drop it, and the child rows report the schemas synced.
func TestSchemaDiffChecksTheWholeDocumentOfEitherSide_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)
	line := fmt.Sprintf(schemaFaults[1].line, fixture.current["dev"]+".parent")

	for _, test := range []struct {
		name     string
		from, to string
		excluded string
		want     string
	}{
		{name: "faulty --from, parent excluded", from: schemaFaults[1].schema, to: referenceParentAlone, excluded: "{schema}.parent", want: "error: " + fromSideRefusal + line + "\n"},
		{name: "faulty --from, child excluded", from: schemaFaults[1].schema, to: unkeyedReferenceSound, excluded: "{schema}.child", want: "error: " + fromSideRefusal + line + "\n"},
		{name: "faulty --to, parent excluded", from: referenceParentAlone, to: schemaFaults[1].schema, excluded: "{schema}.parent", want: "error: " + line + "\n"},
		{name: "faulty --to, child excluded", from: unkeyedReferenceSound, to: schemaFaults[1].schema, excluded: "{schema}.child", want: "error: " + line + "\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := fixture.runDocumentDiff(c, test.from, test.to, "--exclude", test.excluded)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 2, comment)
			c.Assert(result.Stderr, qt.Equals, test.want, comment)
			c.Assert(result.Stdout, qt.Equals, "", comment)
		})
	}
}

// TestSchemaDiffChecksTheWholeDocumentOfEitherSide_HappyPathE2E is the control
// for the rows above: the same exclusions over documents with no fault match
// a table, so the diff compares what is left and reports it synced. An
// exclusion that matched nothing would plan the excluded table here, and warn.
func TestSchemaDiffChecksTheWholeDocumentOfEitherSide_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)

	for _, test := range []struct {
		name     string
		from, to string
		excluded string
	}{
		{name: "child excluded", from: unkeyedReferenceSound, to: referenceParentAlone, excluded: "{schema}.child"},
		{name: "parent excluded", from: unkeyedReferenceSound, to: "CREATE TABLE child (id bigint PRIMARY KEY, code text);\n", excluded: "{schema}.parent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := fixture.runDocumentDiff(c, test.from, test.to, "--exclude", test.excluded)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 0, comment)
			c.Assert(result.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n", comment)
			c.Assert(result.Stderr, qt.Equals, "", comment)
		})
	}
}

// TestSchemaCommandsReadTheCorrectedSchema_HappyPathE2E runs each command on
// [soundSchema], so the refusals above are the faults' doing and not the
// fixture's.
func TestSchemaCommandsReadTheCorrectedSchema_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)

	for _, cmd := range slices.Concat(targetCommands, []targetCommand{fromDocumentDiff}) {
		t.Run(cmd.name, func(t *testing.T) {
			c := qt.New(t)
			result := fixture.run(c, cmd, soundSchema)
			c.Assert(result.ExitCode, qt.Equals, cmd.soundExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		})
	}
}

// TestSchemaCommandsRefuseTheIndexOfAnExcludedTable_FailurePathE2E runs each
// command on [excludedFaultSchema] with t excluded by its placed name. The
// index fault is a fault in the document whatever the run selects, so each
// command refuses it with the line of an unselected run. Checked after the
// selection, drift and compare accept the document and plan and apply refuse
// it, so the four disagree about one document.
func TestSchemaCommandsRefuseTheIndexOfAnExcludedTable_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newFaultFixture(c)
	line := schemaFaults[0].line

	for _, cmd := range targetCommands {
		t.Run(cmd.name, func(t *testing.T) {
			c := qt.New(t)
			result := fixture.run(c, cmd, excludedFaultSchema, "--exclude", "{schema}.t")
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 2, comment)
			c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(line, fixture.current[cmd.placedOn]+".t"), comment)
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
