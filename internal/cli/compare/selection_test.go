package compare_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/compare"
	"ptah.run/internal/cli/internal/exitcode"
)

// selectionDeclared is the application's schema.
const selectionDeclared = "CREATE TABLE subscriptions (id INTEGER PRIMARY KEY, plan TEXT NOT NULL);\n"

// selectionFixture builds a SQLite database from statements, one per line, writes
// the desired schema, and returns what a command names them by.
func selectionFixture(c *qt.C, declared string, statements ...string) (url, schemaFile string) {
	c.Helper()
	dir := c.TempDir()
	schemaFile = filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(schemaFile, []byte(declared), 0o600), qt.IsNil)
	url = "sqlite://" + filepath.ToSlash(filepath.Join(dir, "live.db"))
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	dbschema.CloseAndWarn(conn)
	return url, schemaFile
}

// runCompare runs the command and reports the status it exits with and what each
// stream received.
func runCompare(c *qt.C, args ...string) (code int, stdout, stderr string) {
	c.Helper()
	cmd := compare.NewCompareCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return exitcode.Code(err, 0), out.String(), errOut.String()
}

// historyStatements create what Liquibase keeps in the selected schema, in the
// shape it creates, and what a desired schema never declares.
var historyStatements = []string{
	"CREATE TABLE databasechangeloglock (id INTEGER PRIMARY KEY, locked INTEGER NOT NULL)",
	"CREATE TABLE databasechangelog (id TEXT NOT NULL, author TEXT NOT NULL)",
}

// TestCompare_ASelectorLeavesTheHistoryTablesOutOfTheReport is the issue: the
// reconciling SQL would drop another tool's history until a selector leaves the
// tables out, and then neither the categories, the SQL nor the exit code name them.
func TestCompare_ASelectorLeavesTheHistoryTablesOutOfTheReport(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{name: "each table excluded", flags: []string{"--exclude", "databasechangelog", "--exclude", "databasechangeloglock"}},
		{name: "one value listing both", flags: []string{"--exclude", "databasechangelog,databasechangeloglock"}},
		{name: "a glob", flags: []string{"--exclude", "databasechangelog*[type=table]"}},
		{name: "the application included", flags: []string{"--include", "subscriptions"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, schemaFile := selectionFixture(c, selectionDeclared, append(historyStatements, selectionDeclared)...)
			args := append([]string{"--db-url", url, "--schema-file", schemaFile, "--exit-code"}, test.flags...)

			code, stdout, stderr := runCompare(c, args...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Equals, "")
			c.Assert(stdout, qt.Contains, "No schema differences detected.\n")
			c.Assert(stdout, qt.Not(qt.Contains), "DROP TABLE")
			c.Assert(stdout, qt.Not(qt.Contains), "tables_removed")
		})
	}
}

// TestCompare_WithoutASelectorTheHistoryTablesAreDropped is the control for the
// test above, on the same database: nothing is left out on its own.
func TestCompare_WithoutASelectorTheHistoryTablesAreDropped(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared, append(historyStatements, selectionDeclared)...)

	code, stdout, stderr := runCompare(c, "--db-url", url, "--schema-file", schemaFile, "--exit-code")

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "tables_removed (2): databasechangelog, databasechangeloglock\n")
	c.Assert(stdout, qt.Contains, `DROP TABLE "databasechangelog";`)
	c.Assert(stdout, qt.Contains, `DROP TABLE "databasechangeloglock";`)
}

// TestCompare_ARealDifferenceBesideAnExcludedTableIsReported holds a selector to
// what it names, on the exit code as well as on the report.
func TestCompare_ARealDifferenceBesideAnExcludedTableIsReported(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared+"CREATE TABLE invoices (id INTEGER PRIMARY KEY);\n",
		append(historyStatements, selectionDeclared)...)

	code, stdout, stderr := runCompare(c, "--db-url", url, "--schema-file", schemaFile, "--exit-code",
		"--exclude", "databasechangelog", "--exclude", "databasechangeloglock")

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Differences detected (1 category):\n  tables_added (1): invoices\n")
	c.Assert(stdout, qt.Contains, `CREATE TABLE "invoices"`)
	c.Assert(stdout, qt.Not(qt.Contains), "databasechangelog")
}

// TestCompare_AnExcludeThatMatchesNothingWarnsAndDecidesNothing covers the typo in
// a selector: the report and the exit code are what they would be without it, and
// standard error says the selector protected nothing.
func TestCompare_AnExcludeThatMatchesNothingWarnsAndDecidesNothing(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared, append(historyStatements, selectionDeclared)...)
	_, without, _ := runCompare(c, "--db-url", url, "--schema-file", schemaFile, "--exit-code")

	code, stdout, stderr := runCompare(c, "--db-url", url, "--schema-file", schemaFile, "--exit-code",
		"--exclude", "databasechangelogs")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, without)
	c.Assert(stderr, qt.Equals, "Warning: the --exclude selection matched no objects: \"databasechangelogs\".\n")
}

// TestCompare_ARefusedSelectionPrintsNoComparison covers each refusal: exit 2, and
// no line saying the schemas agree. A comparison of nothing would say so.
func TestCompare_ARefusedSelectionPrintsNoComparison(t *testing.T) {
	const declared = `CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users (id));
`
	tests := []struct {
		name  string
		flags []string
		want  string
	}{
		{
			name:  "an include matching nothing on either side",
			flags: []string{"--include", "nothing_here"},
			want:  `the --include selection matched no objects: "nothing_here"`,
		},
		{
			name:  "an include leaving out what a selected object needs",
			flags: []string{"--include", "orders"},
			want:  `(?s)apply --schema/--include to current schema: the --schema/--include selection drops objects that selected objects depend on:\n  - table "main\.orders" depends on table "main\.users" via a foreign key, but "main\.users" is not selected\n.*`,
		},
		{
			name:  "--schemas and an exclude leaving out what a selected object needs",
			flags: []string{"--schemas", "main", "--exclude", "users"},
			want:  `(?s)apply --schema/--include to current schema: the --schema/--include selection drops objects that selected objects depend on:\n  - table "main\.orders" depends on table "main\.users" via a foreign key, but "main\.users" is not selected\n.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, schemaFile := selectionFixture(c, declared,
				"CREATE TABLE users (id INTEGER PRIMARY KEY)",
				"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users (id))",
			)
			cmd := compare.NewCompareCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(append([]string{"--db-url", url, "--schema-file", schemaFile}, test.flags...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(out.String(), qt.Not(qt.Contains), "No schema differences detected.")
		})
	}
}

// TestCompare_AMalformedSelectorIsRefusedBeforeTheDatabase uses an address nothing
// can answer on and prints nothing to standard output, so an answer about the
// selector is an answer given before the command dialed.
func TestCompare_AMalformedSelectorIsRefusedBeforeTheDatabase(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
		want  string
	}{
		{
			name:  "an exclude too deep for any scope",
			flags: []string{"--exclude", "a.b.c.d"},
			want:  `too many parts in pattern "a\.b\.c\.d".*`,
		},
		{
			name:  "an include of a type no selector can name",
			flags: []string{"--include", "*[type=bogus]"},
			want:  `unsupported Atlas include resource type "bogus" in selector "\*\[type=bogus\]"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := compare.NewCompareCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(append([]string{"--db-url", "postgres://localhost:1/database"}, test.flags...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
