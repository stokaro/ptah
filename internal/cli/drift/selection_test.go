package drift_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/drift"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/safety"
)

// selectionHistoryTables is what Liquibase keeps in the selected schema, in the
// shape it creates, and what a desired schema never declares.
const selectionHistoryTables = `CREATE TABLE databasechangeloglock (id INTEGER PRIMARY KEY, locked INTEGER NOT NULL);
CREATE TABLE databasechangelog (id TEXT NOT NULL, author TEXT NOT NULL);
`

// selectionDeclared is the application's schema, which the database matches.
const selectionDeclared = `CREATE TABLE subscriptions (id INTEGER PRIMARY KEY, plan TEXT NOT NULL);
`

// selectionReport is the part of the JSON report these tests read.
type selectionReport struct {
	Drift     bool                    `json:"drift"`
	Failed    bool                    `json:"failed"`
	Findings  []safety.Finding        `json:"findings"`
	Inventory []safety.InventoryEntry `json:"inventory"`
}

// selectionFixture builds a database from ddl and writes the desired schema, and
// returns what a command names them by.
func selectionFixture(c *qt.C, ddl, declared string) (url, schemaFile string) {
	c.Helper()
	dir := c.TempDir()
	return sqliteDatabase(c, dir, ddl), writeSchemaFile(c, dir, declared)
}

// decodeSelectionReport holds the output to one JSON document.
func decodeSelectionReport(c *qt.C, stdout string) selectionReport {
	c.Helper()
	var report selectionReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	return report
}

// TestRunDrift_ASelectorLeavesTheHistoryTablesOutOfTheReport is the issue: tables
// another tool keeps beside the application read as destructive drift until a
// selector leaves them out, and then they leave the exit code, the findings and
// the inventory together.
func TestRunDrift_ASelectorLeavesTheHistoryTablesOutOfTheReport(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{
			name:  "each table excluded",
			flags: []string{"--exclude", "databasechangelog", "--exclude", "databasechangeloglock"},
		},
		{
			name:  "one value listing both",
			flags: []string{"--exclude", "databasechangelog,databasechangeloglock"},
		},
		{
			name:  "a glob",
			flags: []string{"--exclude", "databasechangelog*[type=table]"},
		},
		{
			name:  "the application included",
			flags: []string{"--include", "subscriptions"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, schemaFile := selectionFixture(c, selectionDeclared+selectionHistoryTables, selectionDeclared)
			args := append([]string{"--db-url", url, "--schema-file", schemaFile, "--format", "json"}, test.flags...)

			code, stdout, stderr := runDriftCommand(c, args...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Equals, "")
			report := decodeSelectionReport(c, stdout)
			c.Assert(report.Drift, qt.IsFalse)
			c.Assert(report.Failed, qt.IsFalse)
			c.Assert(report.Findings, qt.HasLen, 0)
			c.Assert(report.Inventory, qt.DeepEquals, []safety.InventoryEntry{
				{Kind: "columns", Desired: 2, Observed: 2},
				{Kind: "tables", Desired: 1, Observed: 1},
			})
		})
	}
}

// TestRunDrift_EveryFormatReadsTheSelection covers the two formats a person reads.
func TestRunDrift_EveryFormatReadsTheSelection(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{format: "text", want: "No schema drift detected.\n"},
		{format: "github-actions", want: "::notice title=Ptah drift check::No schema drift detected\n"},
	}

	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			c := qt.New(t)
			url, schemaFile := selectionFixture(c, selectionDeclared+selectionHistoryTables, selectionDeclared)

			code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", test.format,
				"--exclude", "databasechangelog", "--exclude", "databasechangeloglock")

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestRunDrift_WithoutASelectorTheHistoryTablesAreDrift is the control for the
// test above, on the same database: nothing is left out on its own.
func TestRunDrift_WithoutASelectorTheHistoryTablesAreDrift(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared+selectionHistoryTables, selectionDeclared)

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json")

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	report := decodeSelectionReport(c, stdout)
	c.Assert(report.Drift, qt.IsTrue)
	c.Assert(report.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "constraints_removed", Count: 1, Severity: safety.Destructive},
		{Category: "tables_removed", Count: 2, Severity: safety.Destructive},
	})
	c.Assert(report.Inventory, qt.DeepEquals, []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 2},
		{Kind: "constraints", Desired: 0, Observed: 1},
		{Kind: "tables", Desired: 1, Observed: 3},
	})
}

// TestRunDrift_ARealDifferenceBesideAnExcludedTableIsReported holds a selector to
// what it names. A selection that hid the whole report would turn the gate green
// on the drift it exists to catch.
func TestRunDrift_ARealDifferenceBesideAnExcludedTableIsReported(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c,
		`CREATE TABLE subscriptions (id INTEGER PRIMARY KEY, plan TEXT NOT NULL, legacy TEXT);
`+selectionHistoryTables,
		selectionDeclared)

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json",
		"--exclude", "databasechangelog", "--exclude", "databasechangeloglock")

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	report := decodeSelectionReport(c, stdout)
	c.Assert(report.Drift, qt.IsTrue)
	c.Assert(report.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "columns_removed", Count: 1, Severity: safety.Destructive},
	})
	c.Assert(report.Inventory, qt.DeepEquals, []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 3},
		{Kind: "tables", Desired: 1, Observed: 1},
	})
}

// TestRunDrift_AnExcludeThatMatchesNothingWarnsAndDecidesNothing covers the typo
// in a selector: the report and the exit code are what they would be without it,
// and standard error says the selector protected nothing.
func TestRunDrift_AnExcludeThatMatchesNothingWarnsAndDecidesNothing(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared+selectionHistoryTables, selectionDeclared)
	_, without, _ := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json")

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json",
		"--exclude", "databasechangelogs")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, without)
	c.Assert(stderr, qt.Equals, "Warning: the --exclude selection matched no objects: \"databasechangelogs\".\n")
}

// TestRunDrift_ARefusedSelectionIsAFailureDocument covers each refusal under
// --format json: the document a reader parses says the check could not run, on
// standard output, and no report takes its place. A selector error says nothing
// about the database, so it has no kind.
func TestRunDrift_ARefusedSelectionIsAFailureDocument(t *testing.T) {
	const foreignKey = `CREATE TABLE users (id INTEGER PRIMARY KEY);
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
		{
			name:  "an exclude too deep for this run's scope",
			flags: []string{"--exclude", "a.b.c"},
			want:  `apply --exclude to current schema: too many parts in pattern "a\.b\.c".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, schemaFile := selectionFixture(c, foreignKey, foreignKey)
			args := append([]string{"--db-url", url, "--schema-file", schemaFile, "--format", "json"}, test.flags...)

			code, stdout, stderr := runDriftCommand(c, args...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			document := decodeOnly[errorkind.Failure](c, stdout)
			c.Assert(document.ContractVersion, qt.Equals, drift.ContractVersion)
			c.Assert(document.Command, qt.Equals, drift.Command)
			c.Assert(document.Error, qt.IsNotNil)
			c.Assert(document.Error.Kind, qt.Equals, errorkind.Kind(""))
			c.Assert(document.Error.Message, qt.Matches, test.want)
		})
	}
}

// TestRunDrift_AMalformedSelectorIsRefusedBeforeTheDatabase uses an address
// nothing listens on, so an answer about the selector is an answer given before
// the command dialed.
func TestRunDrift_AMalformedSelectorIsRefusedBeforeTheDatabase(t *testing.T) {
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
			schemaFile := filepath.Join(c.TempDir(), "desired.sql")
			c.Assert(os.WriteFile(schemaFile, []byte(selectionDeclared), 0o600), qt.IsNil)
			args := append([]string{
				"--db-url", "postgres://ptah:secret@" + refusedAddress(c) + "/app?sslmode=disable",
				"--schema-file", schemaFile, "--format", "json",
			}, test.flags...)

			code, stdout, _ := runDriftCommand(c, args...)

			c.Assert(code, qt.Equals, 2)
			document := decodeOnly[errorkind.Failure](c, stdout)
			c.Assert(document.Error, qt.IsNotNil)
			c.Assert(document.Error.Kind, qt.Equals, errorkind.Kind(""))
			c.Assert(document.Error.Message, qt.Matches, test.want)
		})
	}
}

// TestRunDrift_AnExcludedTableLeavesItsRowsAlone is the row half of the selection.
// The table has left the live side, so a declaration left behind compares with
// nothing and reports every row as an insert.
func TestRunDrift_AnExcludedTableLeavesItsRowsAlone(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	root := managedDataRoot(c, dir, declaredRows)
	url := seededDatabase(c, dir, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--root-dir", root, "--exclude", "countries")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "No schema drift detected.\n")
	c.Assert(stderr, qt.Equals, "")
}

// TestRunDrift_ATableBothFlagsNameIsNoUnmatchedSelector pins the order of the two
// filters: a selector is measured against what the read found, not against what
// --ignore left of it.
func TestRunDrift_ATableBothFlagsNameIsNoUnmatchedSelector(t *testing.T) {
	c := qt.New(t)
	url, schemaFile := selectionFixture(c, selectionDeclared+selectionHistoryTables, selectionDeclared)

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile,
		"--exclude", "databasechangelog", "--exclude", "databasechangeloglock",
		"--ignore", "tables=databasechangelog")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "No schema drift detected.\n")
	c.Assert(stderr, qt.Equals, "")
}
