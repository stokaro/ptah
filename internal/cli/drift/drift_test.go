package drift_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/drift"
	"ptah.run/internal/cli/internal/exitcode"
)

const (
	sqlServerSchemaCommand = "go run ../internal/schemaops/testdata/sqlserver-schema-command"
	sqlServerDatabaseURL   = "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable"
)

func TestNewDriftCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "drift")
	c.Assert(cmd.Short, qt.Contains, "drift")
}

func TestNewDriftCommand_ExposesRepeatableSchemaSources(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()

	rootDir := cmd.Flags().Lookup("root-dir")
	c.Assert(rootDir, qt.IsNotNil)
	c.Assert(rootDir.Value.Type(), qt.Equals, "stringArray")

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")

	c.Assert(cmd.Flags().Lookup("schema-cmd"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("schema-format"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("allow-external-schema"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("plain-http"), qt.IsNotNil)
}

func TestRunDrift_MissingDatabaseURLReturnsCode2(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetArgs([]string{"--root-dir", "."})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Contains, "database URL is required")
}

func TestRunDrift_UsesDatabaseDialectForExternalSQL(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", sqlServerSchemaCommand,
		"--db-url", sqlServerDatabaseURL,
		"--connect-timeout", "1ns",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}

// TestRunDrift_TheReportGoesToStdoutWhetherOrNotThereIsDrift pins the stream
// the document is written to.
//
// It went to stderr when there WAS drift, which put the answer on the error
// stream in exactly the case a reader runs this for: `--format json` piped to a
// parser produced an empty stdout on a drifted database, and the parser saw "no
// output" rather than "drift". The sibling verb settles which is right --
// `migrations status --json --exit-code` writes its document to stdout and
// exits 1, and both verbs are the same contract (stokaro/ptah#852).
//
// Both rows are asserted because a fix that moved the document to stdout and
// left a copy on stderr would satisfy the drifted row alone.
func TestRunDrift_TheReportGoesToStdoutWhetherOrNotThereIsDrift(t *testing.T) {
	tests := []struct {
		name string
		// desired is the schema file the database is compared against.
		desired string
		// wantExit is the process exit code for that comparison.
		wantExit int
		// wantDrift is the value of the document's "drift" field.
		wantDrift string
	}{
		{
			name:      "a drifted database",
			desired:   "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n",
			wantExit:  1,
			wantDrift: `"drift": true`,
		},
		{
			name:      "a converged database",
			desired:   "",
			wantExit:  0,
			wantDrift: `"drift": false`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TB.TempDir()
			schemaFile := filepath.Join(dir, "desired.sql")
			c.Assert(os.WriteFile(schemaFile, []byte(test.desired), 0o600), qt.IsNil)

			cmd := drift.NewDriftCommand()
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{
				"--db-url", "sqlite://" + filepath.Join(dir, "target.db"),
				"--schema-file", schemaFile,
				"--format", "json",
			})

			err := cmd.Execute()

			c.Assert(exitcode.Code(err, 0), qt.Equals, test.wantExit)
			c.Assert(stdout.String(), qt.Contains, test.wantDrift)
			// Nothing on stderr: the document is the answer, and a copy there
			// would make a caller merging the streams read it twice.
			c.Assert(stderr.String(), qt.Equals, "")
		})
	}
}

// managedDataRoot writes a Go entity root declaring one reference table and the
// rows the declaration holds, and returns the root directory.
func managedDataRoot(c *qt.C, dir, rows string) string {
	c.Helper()
	root := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	source := `package entities

//ptah:schema:data table="countries" key="code" file="countries.yaml"
//ptah:schema:table name="countries"
type Country struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(source), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "countries.yaml"), []byte(rows), 0o600), qt.IsNil)
	return root
}

// seededDatabase creates the countries table in a fresh SQLite file and fills it
// with the given code/name pairs, returning the URL to check it with. The
// connection is closed before the URL is handed back, so the command under test
// opens the file itself.
func seededDatabase(c *qt.C, dir string, rows [][2]string) string {
	c.Helper()
	ctx := context.Background()
	url := "sqlite://" + filepath.Join(dir, "app.db")

	conn, err := dbschema.ConnectToDatabase(ctx, url)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, row := range rows {
		_, insertErr := conn.ExecContext(ctx, `INSERT INTO countries (code, name) VALUES (?, ?)`, row[0], row[1])
		c.Assert(insertErr, qt.IsNil)
	}
	dbschema.CloseAndWarn(conn)
	return url
}

// runDriftCommand executes the command with the given arguments and returns the
// exit code and what each stream received.
func runDriftCommand(c *qt.C, args ...string) (code int, stdout, stderr string) {
	c.Helper()
	cmd := drift.NewDriftCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return exitcode.Code(err, 0), out.String(), errOut.String()
}

// declaredRows is the reference data both the drifted and the converged fixture
// declare, so the two runs differ only in what the database holds.
const declaredRows = `
- code: US
  name: United States
- code: CZ
  name: Czechia
`

// TestRunDrift_AHandEditedDeclaredRowIsDrift drives the reproduction from
// stokaro/ptah#3250: the structure matches and somebody edited a reference row
// in the database.
//
// The check has to say so, and it has to say so without publishing the value
// that changed — the caller that asked for this is an operator publishing the
// findings into a status field.
func TestRunDrift_AHandEditedDeclaredRowIsDrift(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	root := managedDataRoot(c, dir, declaredRows)
	url := seededDatabase(c, dir, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--root-dir", root, "--format", "json")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `"drift": true`)
	c.Assert(stdout, qt.Contains, `"highest_severity": "destructive"`)
	c.Assert(stdout, qt.Contains, `"table": "countries"`)
	c.Assert(stdout, qt.Contains, `"updates": 1`)
	c.Assert(stdout, qt.Contains, `"category": "data_rows_updated"`)
	// The structure is identical on both sides, which is what makes this drift
	// invisible to a check that reads the schema diff alone.
	c.Assert(stdout, qt.Contains, `"tables_modified": null`)
	c.Assert(stdout, qt.Not(qt.Contains), "Czechia")
	c.Assert(stdout, qt.Not(qt.Contains), "Czech Republic")
}

// TestRunDrift_DeclaredRowsInPlaceAreNotDrift is the control. Without it the
// test above passes against a check that reports every declared table on every
// run, which is a gate that never goes green again.
func TestRunDrift_DeclaredRowsInPlaceAreNotDrift(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	root := managedDataRoot(c, dir, declaredRows)
	url := seededDatabase(c, dir, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
	})

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--root-dir", root, "--format", "json")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `"drift": false`)
	c.Assert(stdout, qt.Not(qt.Contains), "managed_data")
}

// TestRunDrift_AHandEditedDeclaredRowFailsTheDestructiveThreshold pins what
// --severity destructive sees. An edited row overwrote a value the database
// held, which is the loss the threshold exists to catch, so lowering the
// threshold must not turn this run green.
func TestRunDrift_AHandEditedDeclaredRowFailsTheDestructiveThreshold(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	root := managedDataRoot(c, dir, declaredRows)
	url := seededDatabase(c, dir, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})

	code, stdout, _ := runDriftCommand(c, "--db-url", url, "--root-dir", root, "--severity", "destructive")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "Failure threshold: destructive. Failing: true.")
	c.Assert(stdout, qt.Contains, "Managed data:\n- countries: 0 insert(s), 1 update(s), 0 delete(s)")
	c.Assert(stdout, qt.Contains, "- data_rows_updated: 1 (destructive)")
	c.Assert(stdout, qt.Not(qt.Contains), "Czech Republic")
}

// TestRunDrift_AnIgnoredTableLeavesItsRowsAlone proves --ignore excludes a
// table from the whole check. Excluding it from the structural half and reading
// its rows in the other would make the flag mean half of what it says.
func TestRunDrift_AnIgnoredTableLeavesItsRowsAlone(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	root := managedDataRoot(c, dir, declaredRows)
	url := seededDatabase(c, dir, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})

	code, stdout, _ := runDriftCommand(c, "--db-url", url, "--root-dir", root, "--ignore", "tables=countries")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stdout, qt.Contains, "No schema drift detected.")
}
