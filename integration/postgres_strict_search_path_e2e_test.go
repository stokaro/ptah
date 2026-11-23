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

// PTAH_ATLAS_STRICT_COMPAT=1 reads a URL's search_path as one literal schema
// name, as the pinned community binary does: a space around the value, a
// capital letter and a comma stay part of the name. The default surface reads
// the value with the server's own grammar. Each test below runs against a
// database holding one table in each of the schemas `a`, ` a`, `a ` and `A`,
// so the table a run reads names the schema it selected.

// strictSearchPathSchemas creates the schemas and their tables, and the empty
// schema e a diff starts from.
var strictSearchPathSchemas = []string{
	"CREATE SCHEMA a", `CREATE SCHEMA " a"`, `CREATE SCHEMA "a "`, `CREATE SCHEMA "A"`, "CREATE SCHEMA e",
	"CREATE TABLE a.t_plain (id integer)", `CREATE TABLE " a".t_lead (id integer)`,
	`CREATE TABLE "a ".t_trail (id integer)`, `CREATE TABLE "A".t_upper (id integer)`,
}

// strictSearchPathValues are the search_path spellings, as a URL writes them,
// and the table the pinned binary v1.4.0 reads for each, measured against
// PostgreSQL 18. It refuses `a,b` as `schema "a,b" was not found`, exit 1.
var strictSearchPathValues = []struct {
	value string
	// tables are the CREATE TABLE lines a run prints.
	tables   []string
	exitCode int
}{
	{value: "%20a", tables: []string{`CREATE TABLE "t_lead" ("id" integer NULL);`}},
	{value: "a%20", tables: []string{`CREATE TABLE "t_trail" ("id" integer NULL);`}},
	{value: "A", tables: []string{`CREATE TABLE "t_upper" ("id" integer NULL);`}},
	{value: "a,b", exitCode: 1},
}

// strictSearchPathSources are the places a compatibility run reads a database
// URL from that selects the schema it reads. Each is given the database URL
// with the search_path and the working directory, and returns the command
// line. The working directory holds an atlas.hcl whose env x names that URL.
var strictSearchPathSources = []struct {
	name string
	args func(database, selected string) []string
}{
	{
		name: "-u",
		args: func(_, selected string) []string {
			return []string{"schema", "inspect", "-u", selected, "--format", "{{ sql . }}"}
		},
	},
	{
		name: "--to",
		args: func(database, selected string) []string {
			return []string{"schema", "diff", "--from", withURLParameter(database, "search_path", "e"), "--to", selected}
		},
	},
	{
		name: "the atlas.hcl env url",
		args: func(string, string) []string {
			return []string{"schema", "inspect", "--env", "x", "--format", "{{ sql . }}"}
		},
	},
}

// TestStrictSearchPathSelectsTheLiteralSchemaE2E runs each source with each
// spelling on the pinned binary and on PTAH_ATLAS_STRICT_COMPAT=1, and the two
// read the same table and exit the same way. The wording of the refusal
// differs.
func TestStrictSearchPathSelectsTheLiteralSchemaE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "strict_search_path")
	for _, statement := range strictSearchPathSchemas {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	for _, source := range strictSearchPathSources {
		for _, value := range strictSearchPathValues {
			t.Run(source.name+" "+value.value, func(t *testing.T) {
				c := qt.New(t)
				selected := withURLParameter(database, "search_path", value.value)
				dir := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte("env \"x\" {\n  url = \""+selected+"\"\n}\n"), 0o600), qt.IsNil)
				args := source.args(database, selected)
				referenceOutput, referenceExit := runStrictSearchPathReference(c, reference, dir, args)

				got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")}, args...)

				c.Assert(referenceExit, qt.Equals, value.exitCode, qt.Commentf("%s", referenceOutput))
				c.Assert(got.ExitCode, qt.Equals, value.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, strictSearchPathCreatedTables(referenceOutput))
				c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, value.tables)
			})
		}
	}
}

// TestStrictSearchPathReplaysInTheLiteralSchemaE2E inspects an SQL document
// through a dev database whose URL selects each spelling. The dev database has
// no schema `a`, the one the server's grammar reads for the first three, so a
// run that sent the value unquoted is refused. Strict mode replays the
// document in the schema the value names and reads it back from there, so the
// table appears and the dev database is left empty.
//
// The pinned binary v1.4.0 is not followed here, deliberately. Measured against
// PostgreSQL 18 on a dev database that also holds `a`, it replays the document
// where the server's grammar puts it, in `a`, and reads the literal schema:
// each of the first three spellings prints an empty document, exit 0, and
// leaves the table `a.t` behind in the dev database.
func TestStrictSearchPathReplaysInTheLiteralSchemaE2E(t *testing.T) {
	// The document sits outside the subtests' directories, whose names hold a
	// percent sign a file URL would decode.
	document := writeSpacedSchemaFile(qt.New(t), "schema.sql", "CREATE TABLE t (id integer);\n")
	replayed := []string{`CREATE TABLE "t" ("id" integer NULL);`}
	for _, value := range []struct {
		value    string
		tables   []string
		exitCode int
	}{
		{value: "%20a", tables: replayed},
		{value: "a%20", tables: replayed},
		{value: "A", tables: replayed},
		{value: "a,b", exitCode: 1},
	} {
		t.Run(value.value, func(t *testing.T) {
			c := qt.New(t)
			dev := newEmptyReadScopeDatabase(c, "strict_search_path_dev")
			for _, statement := range []string{`CREATE SCHEMA " a"`, `CREATE SCHEMA "a "`, `CREATE SCHEMA "A"`} {
				execPostgresFamilySQL(c, c.Context(), dev, statement)
			}

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
				"schema", "inspect", "-u", "file://"+filepath.ToSlash(document),
				"--dev-url", withURLParameter(dev, "search_path", value.value), "--format", "{{ sql . }}")

			c.Assert(got.ExitCode, qt.Equals, value.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(strictSearchPathCreatedTables(got.Stdout), qt.DeepEquals, value.tables)
			c.Assert(strictSearchPathDevTables(c, dev), qt.Equals, "")
		})
	}
}

// TestDefaultSearchPathReadsTheServerGrammarE2E is the control: without strict
// mode, the server trims an unquoted name and folds it to lower case, so each
// single-name spelling reads `a`.
func TestDefaultSearchPathReadsTheServerGrammarE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "default_search_path")
	for _, statement := range strictSearchPathSchemas {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	for _, value := range strictSearchPathValues[:3] {
		t.Run(value.value, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
				"schema", "inspect", "-u", withURLParameter(database, "search_path", value.value), "--format", "{{ sql . }}")

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
