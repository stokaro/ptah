//go:build integration

package integration_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// The pinned community binary reads a URL's search_path as one literal schema
// name: a space around the value, a capital letter and a comma stay part of
// the name. The server, and Ptah with it, reads the value with the search_path
// grammar, which trims an unquoted name, folds it to lower case, splits on a
// comma and reads `$user` as the schema named after the session's user.
// PTAH_ATLAS_STRICT_COMPAT=1 refuses a value the two read differently, before
// it connects, and names where the URL came from. The tests below run against
// a database holding one table in each of the schemas `a`, ` a`, `a `, `A`,
// `a,b` and `$user`, so the table a run reads names the schema it selected.

// strictSearchPathSchemas creates the schemas and their tables, and the empty
// schema e a diff starts from.
var strictSearchPathSchemas = []string{
	"CREATE SCHEMA a", `CREATE SCHEMA " a"`, `CREATE SCHEMA "a "`, `CREATE SCHEMA "A"`, `CREATE SCHEMA "a,b"`, `CREATE SCHEMA "$user"`,
	"CREATE SCHEMA e",
	"CREATE TABLE a.t_plain (id integer)", `CREATE TABLE " a".t_lead (id integer)`,
	`CREATE TABLE "a ".t_trail (id integer)`, `CREATE TABLE "A".t_upper (id integer)`, `CREATE TABLE "a,b".t_comma (id integer)`,
	`CREATE TABLE "$user".t_dollar (id integer)`,
}

// strictSearchPathCommands are the places a compatibility run reads a database
// URL from that selects the schema it reads, each with the name the refusal
// gives it. args is given the database URL, the URL with the search_path, and
// returns the command line; the working directory holds an atlas.hcl whose
// env x names the second URL.
var strictSearchPathCommands = map[string]struct {
	label string
	args  func(database, selected string) []string
}{
	"-u": {
		label: "--url",
		args: func(_, selected string) []string {
			return []string{"schema", "inspect", "-u", selected, "--format", "{{ sql . }}"}
		},
	},
	"--to": {
		label: "--to",
		args: func(database, selected string) []string {
			return []string{"schema", "diff", "--from", withURLParameter(database, "search_path", "e"), "--to", selected}
		},
	},
	"the atlas.hcl env url": {
		label: `the url of env "x"`,
		args: func(string, string) []string {
			return []string{"schema", "inspect", "--env", "x", "--format", "{{ sql . }}"}
		},
	},
}

// strictSearchPathRows are the spellings, as a URL writes them, with the
// tables the pinned binary v1.4.0 reads for each and its exit code, measured
// against PostgreSQL 18. It reads `a,b` as the one schema a,b through -u and
// an env url, and refuses it through --to as `missing scheme`, because it
// splits that flag on the comma. It reads `$user` as a schema of that name,
// and refuses it with `schema "$user" was not found` where there is none; the
// server reads the schema named after the session's user.
var strictSearchPathRows = []struct {
	source, value   string
	referenceTables []string
	referenceExit   int
}{
	{source: "-u", value: "%20a", referenceTables: []string{`CREATE TABLE "t_lead" ("id" integer NULL);`}},
	{source: "-u", value: "a%20", referenceTables: []string{`CREATE TABLE "t_trail" ("id" integer NULL);`}},
	{source: "-u", value: "A", referenceTables: []string{`CREATE TABLE "t_upper" ("id" integer NULL);`}},
	{source: "-u", value: "a,b", referenceTables: []string{`CREATE TABLE "t_comma" ("id" integer NULL);`}},
	{source: "-u", value: "$user", referenceTables: []string{`CREATE TABLE "t_dollar" ("id" integer NULL);`}},
	{source: "--to", value: "%20a", referenceTables: []string{`CREATE TABLE "t_lead" ("id" integer NULL);`}},
	{source: "--to", value: "a%20", referenceTables: []string{`CREATE TABLE "t_trail" ("id" integer NULL);`}},
	{source: "--to", value: "A", referenceTables: []string{`CREATE TABLE "t_upper" ("id" integer NULL);`}},
	{source: "--to", value: "a,b", referenceExit: 1},
	{source: "--to", value: "$user", referenceTables: []string{`CREATE TABLE "t_dollar" ("id" integer NULL);`}},
	{source: "the atlas.hcl env url", value: "%20a", referenceTables: []string{`CREATE TABLE "t_lead" ("id" integer NULL);`}},
	{source: "the atlas.hcl env url", value: "a%20", referenceTables: []string{`CREATE TABLE "t_trail" ("id" integer NULL);`}},
	{source: "the atlas.hcl env url", value: "A", referenceTables: []string{`CREATE TABLE "t_upper" ("id" integer NULL);`}},
	{source: "the atlas.hcl env url", value: "a,b", referenceTables: []string{`CREATE TABLE "t_comma" ("id" integer NULL);`}},
	{source: "the atlas.hcl env url", value: "$user", referenceTables: []string{`CREATE TABLE "t_dollar" ("id" integer NULL);`}},
}

// TestStrictSearchPathRefusesWhatTheReferenceReadsLiterallyE2E runs each source
// with each spelling on the pinned binary and under PTAH_ATLAS_STRICT_COMPAT=1.
// The binary selects the schema the value spells byte for byte, which the
// server's grammar does not, and strict mode refuses the value, deliberately:
// following the binary would make a schema with a space or a capital letter
// the current one, and following the server would select a schema the binary
// does not.
func TestStrictSearchPathRefusesWhatTheReferenceReadsLiterallyE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "strict_search_path")
	for _, statement := range strictSearchPathSchemas {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	for _, row := range strictSearchPathRows {
		t.Run(row.source+" "+row.value, func(t *testing.T) {
			c := qt.New(t)
			selected := withURLParameter(database, "search_path", row.value)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte("env \"x\" {\n  url = \""+selected+"\"\n}\n"), 0o600), qt.IsNil)
			command := strictSearchPathCommands[row.source]
			args := command.args(database, selected)
			referenceOutput, referenceExit := runStrictSearchPathReference(c, reference, dir, args)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")}, args...)

			c.Assert(referenceExit, qt.Equals, row.referenceExit, qt.Commentf("%s", referenceOutput))
			c.Assert(strictSearchPathCreatedTables(referenceOutput), qt.DeepEquals, row.referenceTables)
			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "")
			c.Assert(got.Stderr, qt.Contains, "strict compatibility refuses search_path="+row.value+" in "+command.label+": ")
		})
	}
}

// TestStrictSearchPathRefusesBeforeConnectingE2E points every source at a port
// nothing listens on. The refusal arrives instead of a connection error, so
// strict mode decides before it connects, on --dev-url as on the others.
func TestStrictSearchPathRefusesBeforeConnectingE2E(t *testing.T) {
	const unreachable = "postgres://ptah@127.0.0.1:1/db?sslmode=disable"
	selected := withURLParameter(unreachable, "search_path", "A")
	for _, test := range []struct {
		name, label string
		args        []string
	}{
		{name: "-u", label: "--url", args: []string{"schema", "inspect", "-u", selected}},
		{name: "--from", label: "--from", args: []string{"schema", "diff", "--from", selected, "--to", withURLParameter(unreachable, "search_path", "e")}},
		{name: "--to", label: "--to", args: []string{"schema", "diff", "--from", withURLParameter(unreachable, "search_path", "e"), "--to", selected}},
		{name: "--dev-url", label: "--dev-url", args: []string{"schema", "inspect", "-u", "file://schema.sql", "--dev-url", selected}},
		{name: "the atlas.hcl env url", label: `the url of env "x"`, args: []string{"schema", "inspect", "--env", "x"}},
		{name: "the atlas.hcl env dev url", label: `the dev url of env "y"`, args: []string{"schema", "inspect", "--env", "y", "-u", "file://schema.sql"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t (id integer);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(
				"env \"x\" {\n  url = \""+selected+"\"\n}\nenv \"y\" {\n  dev = \""+selected+"\"\n}\n"), 0o600), qt.IsNil)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")}, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "strict compatibility refuses search_path=A in "+test.label+": ")
			c.Assert(got.Stderr, qt.Not(qt.Contains), "connect")
		})
	}
}

// TestStrictSearchPathRefusesADevURLE2E inspects an SQL document through a dev
// database whose URL selects each spelling. Strict mode refuses the dev URL
// and replays nothing, so the dev database is left empty.
//
// The pinned binary v1.4.0 is not followed here either. Measured against
// PostgreSQL 18 on a dev database that also holds `a`, it replays the document
// where the server's grammar puts it, in `a`, and reads the literal schema:
// each of `%20a`, `a%20` and `A` prints an empty document, exit 0, and leaves
// the table `a.t` behind in the dev database. It refuses `a,b` there, because
// the dev database holds no schema of that name.
func TestStrictSearchPathRefusesADevURLE2E(t *testing.T) {
	// The document sits outside the subtests' directories, whose names hold a
	// percent sign a file URL would decode.
	document := writeSpacedSchemaFile(qt.New(t), "schema.sql", "CREATE TABLE t (id integer);\n")
	for _, value := range []string{"%20a", "a%20", "A", "a,b"} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			dev := newEmptyReadScopeDatabase(c, "strict_search_path_dev")
			for _, statement := range []string{"CREATE SCHEMA a", `CREATE SCHEMA " a"`, `CREATE SCHEMA "a "`, `CREATE SCHEMA "A"`} {
				execPostgresFamilySQL(c, c.Context(), dev, statement)
			}

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
				"schema", "inspect", "-u", "file://"+filepath.ToSlash(document),
				"--dev-url", withURLParameter(dev, "search_path", value), "--format", "{{ sql . }}")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "strict compatibility refuses search_path="+value+" in --dev-url: ")
			c.Assert(strictSearchPathDevTables(c, dev), qt.Equals, "")
		})
	}
}

// TestStrictSearchPathAcceptsAPlainNameE2E is the control for the refusals: a
// lower-case name both readings agree on selects the same schema on the pinned
// binary and under strict mode.
func TestStrictSearchPathAcceptsAPlainNameE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "strict_search_path_plain")
	for _, statement := range strictSearchPathSchemas {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	dir := c.TempDir()
	args := []string{"schema", "inspect", "-u", withURLParameter(database, "search_path", "a"), "--format", "{{ sql . }}"}
	referenceOutput, referenceExit := runStrictSearchPathReference(c, reference, dir, args)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")}, args...)

	c.Assert(referenceExit, qt.Equals, 0, qt.Commentf("%s", referenceOutput))
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, []string{`CREATE TABLE "t_plain" ("id" integer NULL);`})
	c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, strictSearchPathCreatedTables(referenceOutput))
}

// TestDefaultSearchPathReadsTheServerGrammarE2E is the control for the
// surface: without strict mode, the server trims an unquoted name and folds it
// to lower case, so each single-name spelling reads `a`.
func TestDefaultSearchPathReadsTheServerGrammarE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "default_search_path")
	for _, statement := range strictSearchPathSchemas {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	for _, value := range []string{"%20a", "a%20", "A"} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
				"schema", "inspect", "-u", withURLParameter(database, "search_path", value), "--format", "{{ sql . }}")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, []string{`CREATE TABLE "t_plain" ("id" integer NULL);`})
		})
	}
}

// runStrictSearchPathReference runs the pinned binary in dir and returns its
// combined output and exit code.
func runStrictSearchPathReference(c *qt.C, reference, dir string, args []string) (string, int) {
	c.Helper()
	// #nosec G204 G702 -- the reference path is operator-provided via PTAH_ATLAS_REFERENCE
	command := exec.CommandContext(c.Context(), reference, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		c.Fatalf("run the pinned binary: %v", err)
	}
	return string(output), command.ProcessState.ExitCode()
}

// strictSearchPathCreatedTables is the CREATE TABLE lines of output, in
// order, or nil.
func strictSearchPathCreatedTables(output string) []string {
	var tables []string
	for line := range strings.Lines(output) {
		if strings.HasPrefix(line, "CREATE TABLE ") {
			tables = append(tables, strings.TrimSpace(line))
		}
	}
	return tables
}

// strictSearchPathDevTables lists the tables of database outside the system
// schemas as [schema].table, in name order, or "" when there are none.
func strictSearchPathDevTables(c *qt.C, database string) string {
	c.Helper()
	var tables string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg('[' || table_schema || '].' || table_name, ', ' ORDER BY table_schema, table_name), '')
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables), qt.IsNil)
	return tables
}
