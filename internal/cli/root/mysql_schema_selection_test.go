package root_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// These native verbs read an explicitly selected schema from a live URL.
// A database component in a MySQL-family URL limits that read to one database;
// asking for another must not succeed while silently leaving it out (#6558).
var mysqlSchemaSelectionVerbs = []struct {
	name string
	args []string
}{
	{name: "apply", args: []string{"schema", "apply", "--db-url", "<url>", "--schema-file", "<schema>", "--auto-approve"}},
	{name: "plan", args: []string{"schema", "plan", "--db-url", "<url>", "--schema-file", "<schema>", "--output", "<plan>"}},
	{name: "diff from", args: []string{"schema", "diff", "--from", "<url>", "--to", "<schema>"}},
	{name: "diff padded URL", args: []string{"schema", "diff", "--from", "<padded-url>", "--to", "<schema>"}},
	{name: "diff to", args: []string{"schema", "diff", "--from", "<schema>", "--to", "<url>"}},
	{name: "inspect", args: []string{"schema", "inspect", "--db-url", "<url>"}},
	{name: "inspect padded URL", args: []string{"schema", "inspect", "--db-url", "<padded-url>"}},
	{name: "compare", args: []string{"schema", "compare", "--db-url", "<url>", "--schema-file", "<schema>"}},
	{name: "drift", args: []string{"schema", "drift", "--db-url", "<url>", "--schema-file", "<schema>"}},
	{name: "migrations plan", args: []string{"migrations", "plan", "--db-url", "<url>", "--schema-file", "<schema>"}},
	{name: "migrations generate", args: []string{"migrations", "generate", "--db-url", "<url>", "--schema-file", "<schema>", "--migrations-dir", "<migrations>"}},
	{name: "db read", args: []string{"db", "read", "--db-url", "<url>"}},
	{name: "introspect", args: []string{"introspect", "--db-url", "<url>", "--out", "<models>"}},
	{name: "lineage", args: []string{"schema", "lineage", "--db-url", "<url>"}},
	{name: "security", args: []string{"schema", "security", "--db-url", "<url>"}},
	{name: "stats", args: []string{"schema", "stats", "--db-url", "<url>"}},
}

// mysqlSelectionArgs gives a command real local inputs and a closed TCP port.
// A connection error therefore distinguishes an accepted selection from a
// refusal made before connecting, without a live server or timing assertion.
func mysqlSelectionArgs(c *qt.C, args []string, database string) []string {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id INT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	address := refusedMySQLServer(c) + database
	replace := strings.NewReplacer(
		"<url>", address,
		"<padded-url>", " "+address+" ",
		"<schema>", schema,
		"<plan>", filepath.Join(dir, "plan.json"),
		"<migrations>", filepath.Join(dir, "migrations"),
		"<models>", filepath.Join(dir, "models"),
	)
	resolved := slices.Clone(args)
	for i, arg := range resolved {
		resolved[i] = replace.Replace(arg)
	}
	return resolved
}

// Conflicting selections are refused before dialing, for every native read
// and comparison path. The same guard covers both sides of schema diff.
func TestNativeMySQLSchemasOutsideURL_FailurePath(t *testing.T) {
	for _, verb := range mysqlSchemaSelectionVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())
			unsetSchemaVariables(c)
			args := append(mysqlSelectionArgs(c, verb.args, "m1"), "--schemas", "m1,m2")

			_, _, err := executeRootCommand(args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*schema selection names database "m2", but the URL is limited to database "m1"; remove the database from the URL.*`)
		})
	}
}

// Naming only the database the URL already selects remains valid. It reaches
// the closed port, proving that the new refusal did not reject that selection.
func TestNativeMySQLSchemasMatchingURLReachTheDatabase(t *testing.T) {
	for _, verb := range mysqlSchemaSelectionVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())
			unsetSchemaVariables(c)
			args := append(mysqlSelectionArgs(c, verb.args, "m1"), "--schemas", "m1")

			_, _, err := executeRootCommand(args...)

			c.Assert(err, qt.ErrorAs, new(*net.OpError))
		})
	}
}

// Even a conflict on --to is known before opening a valid --from. A guard
// inside the second source's reader would be too late and reach the first port.
func TestNativeMySQLSchemasDiffRefusesSecondURLBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	c.Chdir(c.TempDir())
	unsetSchemaVariables(c)
	from := refusedMySQLServer(c) + "m1"
	to := refusedMySQLServer(c) + "m2"

	_, _, err := executeRootCommand("schema", "diff", "--from", from, "--to", to, "--schemas", "m1")

	c.Assert(err, qt.ErrorMatches, `(?s).*schema selection names database "m1", but the URL is limited to database "m2"; remove the database from the URL.*`)
}

// The effective list is checked, whether it came from a flag, its environment
// twin, or the project. A partial selection must not be accepted only because
// automation supplied it rather than a command line.
func TestNativeMySQLSchemasFromConfiguration_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		env     map[string]string
	}{
		{name: "environment", env: map[string]string{"PTAH_SCHEMAS": "m1,m2"}},
		{name: "project", project: "schemas: [m1, m2]\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Chdir(dir)
			unsetSchemaVariables(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(test.project), 0o600), qt.IsNil)
			args := mysqlSelectionArgs(c, mysqlSchemaSelectionVerbs[0].args, "m1")

			_, _, err := executeRootCommand(args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*schema selection names database "m2", but the URL is limited to database "m1"; remove the database from the URL.*`)
		})
	}
}

// A flag overrides the project's and the environment's conflicting defaults;
// the guard judges the same effective list the subsequent read receives.
func TestNativeMySQLSchemasFlagOverridesConfiguration(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Chdir(dir)
	unsetSchemaVariables(c)
	c.Setenv("PTAH_SCHEMAS", "m1,m2")
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("schemas: [m1, m3]\n"), 0o600), qt.IsNil)
	args := append(mysqlSelectionArgs(c, mysqlSchemaSelectionVerbs[0].args, "m1"), "--schemas", "m1")

	_, _, err := executeRootCommand(args...)

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
}

// A URL from the project is subject to the same selection rule as --db-url.
func TestNativeMySQLSchemaSelectionChecksTheProjectURL(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Chdir(dir)
	unsetSchemaVariables(c)
	address := refusedMySQLServer(c) + "m1"
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), fmt.Appendf(nil,
		"url: %q\nschemas: [m1, m2]\n", address), 0o600), qt.IsNil)
	args := mysqlSelectionArgs(c, []string{"schema", "apply", "--schema-file", "<schema>", "--auto-approve"}, "m1")

	_, _, err := executeRootCommand(args...)

	c.Assert(err, qt.ErrorMatches, `(?s).*schema selection names database "m2", but the URL is limited to database "m1"; remove the database from the URL.*`)
}

// A saved plan uses its recorded scope. Project defaults that are irrelevant
// to that plan must not refuse it before the normal target verification runs.
func TestNativeMySQLSavedPlanKeepsItsRecordedSchemaSelection(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Chdir(dir)
	unsetSchemaVariables(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("schemas: [m1, m2]\n"), 0o600), qt.IsNil)
	document, err := atlasschema.MarshalPlanFile(atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "add_items",
		Dialect:         "mysql",
		Schemas:         []string{"m1"},
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 1),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 2),
		Statements: []atlasschema.PlanStatement{{
			SQL: "CREATE TABLE items (id int)", Severity: safety.Safe, Reason: "create table items",
		}},
	})
	c.Assert(err, qt.IsNil)
	plan := filepath.Join(dir, "plan.json")
	c.Assert(os.WriteFile(plan, document, 0o600), qt.IsNil)

	_, _, err = executeRootCommand("schema", "apply", "--db-url", refusedMySQLServer(c)+"m1", "--plan", plan, "--auto-approve")

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
}
