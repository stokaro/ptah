package schema_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/dbschema"
)

var errTestWrite = errors.New("test writer failed")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

func writeUsersModel(c *qt.C, dir string) {
	c.Helper()
	content := `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "user.go"), []byte(content), 0o600), qt.IsNil)
}

// runSchemaTestCommand runs "schema test" through the full command tree so
// registration in NewSchemaCommand is exercised alongside the command itself.
func runSchemaTestCommand(args ...string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"test"}, args...))
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestSchemaTestCommand_Passes(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users schema works\n"+
			"    steps:\n"+
			"      - name: starts empty\n"+
			"        assert:\n"+
			"          query: SELECT * FROM users\n"+
			"          row_count: 0\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - name: one user\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"1\"\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "PASS  case \"users schema works\"")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestSchemaTestCommand_DefaultSeedDirectory(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	seedsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_users.test.sql"),
		[]byte("INSERT INTO users (id, name) VALUES (1, 'ada');"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "seed.yaml"), []byte(
		"cases:\n"+
			"  - name: default seed directory works\n"+
			"    steps:\n"+
			"      - seed:\n"+
			"          env: test\n"+
			"      - assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand(
		"--dir", testsDir,
		"--root-dir", modelsDir,
		"--seed-dir", seedsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "default seed directory works"`)
}

func TestSchemaTestCommand_ReportWriteFailure(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "pass.yaml"), []byte(
		"cases:\n"+
			"  - name: passing case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM users\n"+
			"          row_count: 0\n"), 0o600), qt.IsNil)
	cmd := schema.NewSchemaCommand()
	cmd.SetOut(failingWriter{})
	cmd.SetArgs([]string{"test", "--dir", testsDir, "--root-dir", modelsDir})

	err := cmd.ExecuteContext(context.Background())

	c.Assert(err, qt.ErrorIs, errTestWrite)
}

func TestSchemaTestCommand_FailsWithNonZeroError(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: bad expectation\n"+
			"    steps:\n"+
			"      - name: wrong count\n"+
			"        assert:\n"+
			"          query: SELECT * FROM users\n"+
			"          row_count: 5\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "schema tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "FAIL  case \"bad expectation\"")
}

func TestSchemaTestCommand_NoCasesFound(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	_, err := runSchemaTestCommand("--dir", t.TempDir(), "--root-dir", modelsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no test cases found")
}

func TestSchemaTestCommand_RejectsUnsupportedReport(t *testing.T) {
	c := qt.New(t)
	_, err := runSchemaTestCommand("--dir", t.TempDir(), "--report", "xml")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unsupported report format")
}

func TestSchemaTestCommand_RunPattern(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	writeUsersModel(c, modelsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: selected case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM users\n"+
			"          row_count: 0\n"+
			"  - name: excluded case\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT id FROM missing_table\n"+
			"          row_count: 0\n"), 0o600), qt.IsNil)

	out, err := runSchemaTestCommand("--dir", testsDir, "--root-dir", modelsDir, "--run", "^selected")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "selected case"`)
	c.Assert(out, qt.Not(qt.Contains), "excluded case")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// liveSourceFixture is the discriminating fixture for a database desired-state
// source. The asserted table exists in the live database and in NO other
// source, so a pass can only come from having introspected it -- with every
// source declaring the same table, a run would pass whether or not the source
// was ever read.
type liveSourceFixture struct {
	liveURL   string
	modelsDir string
	testsDir  string
}

// writeLiveSourceFixture builds the fixture, creating the live database by
// executing DDL over a Ptah SQLite connection rather than committing a binary
// database file.
func writeLiveSourceFixture(c *qt.C) liveSourceFixture {
	c.Helper()
	dir := c.TempDir()
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(`package models

//ptah:schema:table name="users_from_go"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	testsDir := filepath.Join(dir, "tests")
	c.Assert(os.MkdirAll(testsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "db.yaml"), []byte(`cases:
  - name: db-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM orders_from_db"
          row_count: 1
`), 0o600), qt.IsNil)

	livePath := filepath.Join(dir, "live.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+livePath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(),
		"CREATE TABLE orders_from_db (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)

	return liveSourceFixture{
		liveURL:   "sqlite://" + livePath,
		modelsDir: modelsDir,
		testsDir:  testsDir,
	}
}

// runFixtureCases runs the fixture's case file against one desired-state
// source.
//
// Every direction gets its own throwaway database. A shared one is not reset
// between runs, so the control below would pass against a database an earlier
// positive run had already populated.
func runFixtureCases(c *qt.C, fixture liveSourceFixture, source string) (string, error) {
	c.Helper()
	return runSchemaTestCommand(
		"--dir", fixture.testsDir,
		"--root-dir", source,
		"--db-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
	)
}

// TestSchemaTestCommand_DatabaseURLIsIntrospected covers the third source kind:
// a database URL whose live schema becomes the desired state.
//
// The asserted table exists only in the live database, so a pass here can come
// from nothing but having introspected it.
func TestSchemaTestCommand_DatabaseURLIsIntrospected(t *testing.T) {
	c := qt.New(t)
	fixture := writeLiveSourceFixture(c)

	out, err := runFixtureCases(c, fixture, fixture.liveURL)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "db-sourced table exists"`)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaTestCommand_GoAnnotationDirectoryIsTheControl is the counterpart of
// the introspection above: the same case file, pointed at the Go-annotation
// directory, must keep reaching the annotation scan and therefore must NOT find
// the database-only table.
//
// Without it, a source resolver that quietly introspected the live database
// whatever it was handed would pass the test above.
func TestSchemaTestCommand_GoAnnotationDirectoryIsTheControl(t *testing.T) {
	c := qt.New(t)
	fixture := writeLiveSourceFixture(c)

	out, err := runFixtureCases(c, fixture, fixture.modelsDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "no such table: orders_from_db")
}

// TestSchemaTestCommand_RefusesCrossDialectDatabaseSource pins the dialect gate.
//
// The source URL is syntactically valid but unreachable on purpose: asserting
// that the error names the dialect mismatch rather than a connection failure is
// what proves the gate runs before anything is contacted. Without it a
// PostgreSQL source applied to the ephemeral SQLite default reports a green run
// for semantics it never exercised.
func TestSchemaTestCommand_RefusesCrossDialectDatabaseSource(t *testing.T) {
	// No credentials in the URL: the gate reads the scheme only, and a
	// password-shaped literal is a hardcoded-credential lint finding.
	const source = "postgres://127.0.0.1:1/nope?sslmode=disable"
	// The throwaway database of the first row, named before the table so the row
	// can carry the value rather than the means of producing it. It is a path
	// that is never created: the gate refuses ahead of every connection.
	sqliteDevURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	tests := []struct {
		name   string
		devURL string
		want   string
	}{
		{
			name:   "explicit SQLite throwaway database",
			devURL: sqliteDevURL,
			want:   `--db-url dialect "sqlite" does not match --root-dir database dialect "postgres"`,
		},
		{
			name:   "ephemeral SQLite default",
			devURL: "",
			want: `--root-dir database dialect "postgres" requires an explicit --db-url throwaway database` +
				` of the same dialect, because the default ephemeral test database is SQLite`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeLiveSourceFixture(c)

			out, err := runSchemaTestCommand(
				"--dir", fixture.testsDir,
				"--root-dir", source,
				"--db-url", tt.devURL,
			)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Equals, tt.want)
		})
	}
}

func TestSchemaTestCommand_RunPatternNoMatches(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: existing case\n"+
			"    steps:\n"+
			"      - exec: SELECT 1\n"), 0o600), qt.IsNil)

	_, err := runSchemaTestCommand("--dir", testsDir, "--run", "^missing$")

	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^missing\$"`)
}

// runScopedFixtureCases runs the fixture's case file against its live database
// source, restricted to one schema.
func runScopedFixtureCases(c *qt.C, fixture liveSourceFixture, schemaName string) (string, error) {
	c.Helper()
	return runSchemaTestCommand(
		"--dir", fixture.testsDir,
		"--root-dir", fixture.liveURL,
		"--schema", schemaName,
		"--db-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
	)
}

// TestSchemaTestCommand_SchemaSelectionRefusesADatabaseSourceItEmpties pins that
// --schema restricts a desired schema that came from a database URL, and not
// only one parsed from a directory or a schema file.
//
// The source kinds resolve on different branches of resolveTestDesiredSchema:
// the database branch arrived with stokaro/ptah#1110 and the schema selection
// with stokaro/ptah#951, and the two met for the first time when those changes
// were merged. A selection wired into only the file and directory branches
// would leave a database source silently unscoped -- the accept-and-ignore
// shape this flag exists to prevent -- and no test covered that combination.
//
// This is the discriminating direction. An unscoped database source keeps its
// table and the run passes, so the refusal is reachable only when the selection
// actually reached the database branch. The control lives in
// TestSchemaTestCommand_SchemaSelectionKeepsTheSchemaHoldingTheTable, and
// without it "refuse every --schema against a database" would pass here.
func TestSchemaTestCommand_SchemaSelectionRefusesADatabaseSourceItEmpties(t *testing.T) {
	c := qt.New(t)
	fixture := writeLiveSourceFixture(c)

	_, err := runScopedFixtureCases(c, fixture, "nosuch")

	c.Assert(err, qt.ErrorMatches, `--schema nosuch selects no tables out of the desired schema`)
}

// TestSchemaTestCommand_SchemaSelectionKeepsTheSchemaHoldingTheTable is the
// control for the refusal above: naming the schema the table really lives in
// leaves the run passing.
func TestSchemaTestCommand_SchemaSelectionKeepsTheSchemaHoldingTheTable(t *testing.T) {
	c := qt.New(t)
	fixture := writeLiveSourceFixture(c)

	out, err := runScopedFixtureCases(c, fixture, "main")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}
