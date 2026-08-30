package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeItemsSchemaFile writes a SQL desired schema whose table exists in no
// other source, so a pass can come from nothing but having read this file.
func writeItemsSchemaFile(c *qt.C, dir string) string {
	c.Helper()
	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte(
		"CREATE TABLE items_from_file (id INTEGER PRIMARY KEY, label TEXT NOT NULL);\n",
	), 0o600), qt.IsNil)
	return path
}

// writeItemsCases writes the case file that asserts the schema-file table.
func writeItemsCases(c *qt.C, dir string) string {
	c.Helper()
	testsDir := filepath.Join(dir, "tests")
	c.Assert(os.MkdirAll(testsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "items.yaml"), []byte(`cases:
  - name: file-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM items_from_file"
          row_count: 1
`), 0o600), qt.IsNil)
	return testsDir
}

// TestSchemaTestCommand_SchemaFileSelectsTheFile covers the selector whose name
// does not imply a directory (stokaro/ptah#2571).
//
// The asserted table exists only in the file, so a pass here comes from nothing
// but having loaded it through --schema-file.
func TestSchemaTestCommand_SchemaFileSelectsTheFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaPath := writeItemsSchemaFile(c, dir)
	testsDir := writeItemsCases(c, dir)

	out, err := runSchemaTestCommand("--dir", testsDir, "--schema-file", schemaPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "file-sourced table exists"`)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaTestCommand_SourceDBURLIntrospectsTheDatabase covers the selector
// for a live desired-state source.
//
// It is a separate flag from --db-url because that one names the throwaway
// database the cases run against. One URL each, and neither can be mistaken for
// the other.
func TestSchemaTestCommand_SourceDBURLIntrospectsTheDatabase(t *testing.T) {
	c := qt.New(t)
	fixture := writeLiveSourceFixture(c)

	out, err := runSchemaTestCommand(
		"--dir", fixture.testsDir,
		"--source-db-url", fixture.liveURL,
		"--db-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "db-sourced table exists"`)
}

// TestSchemaTestCommand_ConflictingSourceSelectors_FailurePath refuses more
// than one desired-schema selector.
//
// The throwaway database path is asserted absent afterwards, which is the half
// of the contract an error message cannot prove: the refusal has to land before
// anything is provisioned, not after a container or a file has been created and
// left behind.
func TestSchemaTestCommand_ConflictingSourceSelectors_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		extra   []string
		wantErr string
	}{
		{
			name:    "root dir and schema file",
			extra:   []string{"--root-dir", "./models", "--schema-file", "schema.sql"},
			wantErr: "--root-dir and --schema-file name the desired schema together; pass exactly one",
		},
		{
			name:    "root dir and source database",
			extra:   []string{"--root-dir", "./models", "--source-db-url", "sqlite://source.db"},
			wantErr: "--root-dir and --source-db-url name the desired schema together; pass exactly one",
		},
		{
			name:    "schema file and source database",
			extra:   []string{"--schema-file", "schema.sql", "--source-db-url", "sqlite://source.db"},
			wantErr: "--schema-file and --source-db-url name the desired schema together; pass exactly one",
		},
		{
			name: "all three",
			extra: []string{
				"--root-dir", "./models",
				"--schema-file", "schema.sql",
				"--source-db-url", "sqlite://source.db",
			},
			wantErr: "--root-dir, --schema-file and --source-db-url name the desired schema together; pass exactly one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			testsDir := writeItemsCases(c, dir)
			devPath := filepath.Join(dir, "dev.db")

			args := append([]string{"--dir", testsDir, "--db-url", "sqlite://" + devPath}, test.extra...)
			out, err := runSchemaTestCommand(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
			_, statErr := os.Stat(devPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue,
				qt.Commentf("throwaway database was provisioned before the refusal; output:\n%s", out))
		})
	}
}

// TestSchemaTestCommand_SourceDBURLRefusesWhatIsNotADatabase keeps the live
// selector exact.
//
// Without it --source-db-url would be a second spelling of --schema-file for
// anything the classifier reads as a file, and the flag that exists to remove
// an ambiguity would carry one of its own.
func TestSchemaTestCommand_SourceDBURLRefusesWhatIsNotADatabase(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaPath := writeItemsSchemaFile(c, dir)
	testsDir := writeItemsCases(c, dir)

	out, err := runSchemaTestCommand("--dir", testsDir, "--source-db-url", schemaPath)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "is not a database URL")
}

// TestSchemaTestCommand_RootDirNamingAFileReportsTheExactSelector covers the
// pre-GA transition.
//
// The run still works, because refusing it would break an invocation this
// repository documented as supported. It says so on stderr and names the flag
// that selects the source exactly, because the flag name is what made the
// invocation surprising in the first place.
func TestSchemaTestCommand_RootDirNamingAFileReportsTheExactSelector(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaPath := writeItemsSchemaFile(c, dir)
	testsDir := writeItemsCases(c, dir)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", schemaPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "--schema-file selects it exactly")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}
