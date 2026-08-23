package drift_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/drift"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
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
