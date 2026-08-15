package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/dbschema"
)

// writeSchemaTestFixture fills modelsDir with an annotated Go entity and
// testsDir with a passing Ptah-native YAML test case.
func writeSchemaTestFixture(c *qt.C, modelsDir, testsDir string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users schema works\n"+
			"    steps:\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - name: the user is named ada\n"+
			"        assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)
}

func TestCompatCommand_SchemaTestForwardsToNative(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"-u", "file://" + modelsDir,
		"--dev-url", devDB,
	})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah schema test`: -u/--url maps to the
	// native Go-annotation --root-dir, --dev-url to the native throwaway
	// --db-url, and the positional path to the native test-case --dir.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users schema works"`)
	c.Assert(out.String(), qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestCompatCommand_SchemaTestFailingCaseExits1(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: failing expectation\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"42\"\n"), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", testsDir, "--url", "file://" + modelsDir})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "schema tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out.String(), qt.Contains, `FAIL  case "failing expectation"`)
}

func TestCompatCommand_SchemaTestRunFilterSelectsCases(t *testing.T) {
	c := qt.New(t)
	modelsDir, testsDir := t.TempDir(), t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"--url", "file://" + modelsDir,
		"--run", "^does-not-match$",
	})

	err := cmd.Execute()

	// The Atlas --run filter forwards to the native --run pattern; a pattern
	// with no matches fails loudly instead of reporting an empty pass.
	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^does-not-match\$"`)
}

// TestCompatCommand_SchemaTestUnusableDatabaseSourceFailsLoudly replaces a test
// that pinned the refusal this change removes.
//
// A database URL used to be rejected outright as "only local file:// migration
// directories are supported", so the old test asserted that message. Database
// URLs are a supported desired-state source now, which makes the guarantee
// worth pinning a different one: an unusable database source must still fail
// loudly and non-zero, never report a green run it did not perform.
func TestCompatCommand_SchemaTestUnusableDatabaseSourceFailsLoudly(t *testing.T) {
	tests := []struct {
		name string
		// The source is built from the fixture because its paths exist only
		// once the fixture is written; the closure asserts nothing.
		source func(fixture compatLiveSourceFixture) string
		want   string
	}{
		{
			name:   "dialect mismatch is named before anything is contacted",
			source: func(compatLiveSourceFixture) string { return "postgres://127.0.0.1:1/nope?sslmode=disable" },
			want:   `--db-url dialect "sqlite" does not match --root-dir database dialect "postgres"`,
		},
		{
			name:   "an unopenable database of the matching dialect fails on the connection",
			source: func(fixture compatLiveSourceFixture) string { return "sqlite://" + fixture.modelsDir },
			want:   "connect to --root-dir database: failed to ping database: unable to open database file (14)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeCompatLiveSourceFixture(c)

			out, err := runCompatArgs([]string{
				"schema", "test", fixture.testsDir,
				"-u", tt.source(fixture),
				"--dev-url", freshDevURL(c),
			})

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Equals, tt.want)
			c.Assert(out, qt.Not(qt.Contains), "1 passed")
		})
	}
}

func TestCompatCommand_SchemaTestUsesAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	testsDir := t.TempDir()
	writeSchemaTestFixture(c, modelsDir, testsDir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = "file://models"
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "dev.db"))+`"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", testsDir, "--env", "local"})

	err := cmd.Execute()

	// env schema.src supplies the desired schema URL and env dev the throwaway
	// database; env url (the target database URL) is deliberately never
	// injected into the desired schema flag.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users schema works"`)
}

func TestCompatCommand_SchemaTestRejectsMultipleProjectSchemaSources(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  src = ["file://a", "file://b"]
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas schema test supports one atlas.hcl schema source, got 2`)
}

func TestNewCompatCommand_SchemaTestResolvesAtRoot(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--help"})

	err := cmd.Execute()

	// The verb resolves as a working forward through the compatibility binary.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas schema test [flags] [paths]")
	c.Assert(out.String(), qt.Contains, "-u, --url")
}

// writeDistinctSourceFixture writes three desired-schema sources that each
// declare a DIFFERENT table, plus a test case asserting the SQL-sourced one.
//
// The distinct table names are the point. With every source declaring the same
// table, a run passes whether or not the source was read at all, so it would
// not show that -u resolved anything.
func writeDistinctSourceFixture(c *qt.C, dir, testsDir string) (sqlFile, hclFile, modelsDir string) {
	c.Helper()
	modelsDir = filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(`package models

//ptah:schema:table name="users_from_go"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	sqlFile = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(sqlFile,
		[]byte("CREATE TABLE orders_from_sql (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	hclFile = filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(hclFile, []byte(`schema "main" {
}
table "widgets_from_hcl" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	c.Assert(os.WriteFile(filepath.Join(testsDir, "sql.yaml"), []byte(`cases:
  - name: sql-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM orders_from_sql"
          row_count: 1
`), 0o600), qt.IsNil)
	return sqlFile, hclFile, modelsDir
}

func runCompatSchemaTest(c *qt.C, testsDir, source string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "test", testsDir,
		"-u", "file://" + source,
		"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"),
	})
	return func() (string, error) { err := cmd.Execute(); return out.String(), err }()
}

// TestCompatCommand_SchemaTestAcceptsSQLFileSource covers a .sql desired schema.
// The Go-annotation directory is the control: it declares a different table, so
// the same case fails against it. Without that half, a pass would not
// distinguish "read the SQL file" from "read anything at all".
func TestCompatCommand_SchemaTestAcceptsSQLFileSource(t *testing.T) {
	c := qt.New(t)
	dir, testsDir := t.TempDir(), t.TempDir()
	sqlFile, _, modelsDir := writeDistinctSourceFixture(c, dir, testsDir)

	out, err := runCompatSchemaTest(c, testsDir, sqlFile)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")

	controlOut, controlErr := runCompatSchemaTest(c, testsDir, modelsDir)
	c.Assert(controlErr, qt.IsNotNil)
	c.Assert(controlOut, qt.Contains, "no such table: orders_from_sql")
}

// TestCompatCommand_SchemaTestAcceptsHCLFileSource covers a .hcl desired schema.
func TestCompatCommand_SchemaTestAcceptsHCLFileSource(t *testing.T) {
	c := qt.New(t)
	dir, testsDir := t.TempDir(), t.TempDir()
	_, hclFile, _ := writeDistinctSourceFixture(c, dir, testsDir)
	c.Assert(os.Remove(filepath.Join(testsDir, "sql.yaml")), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "hcl.yaml"), []byte(`cases:
  - name: hcl-sourced table exists
    steps:
      - assert:
          query: "SELECT count(*) FROM widgets_from_hcl"
          row_count: 1
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaTest(c, testsDir, hclFile)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// compatLiveSourceFixture is the discriminating fixture for a database
// desired-state source at the CLI.
//
// The asserted table exists in the live database and in NO other source. That
// is the whole point: with every source declaring the same table a run passes
// whether or not the source was read at all.
type compatLiveSourceFixture struct {
	liveURL   string
	modelsDir string
	sqlFile   string
	testsDir  string
}

// writeCompatLiveSourceFixture builds the fixture, creating the live database
// by executing DDL over a Ptah SQLite connection rather than committing a
// binary database file.
func writeCompatLiveSourceFixture(c *qt.C) compatLiveSourceFixture {
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

	sqlFile := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(sqlFile,
		[]byte("CREATE TABLE widgets_from_sql (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

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

	return compatLiveSourceFixture{
		// Slashed: this URL is interpolated into an atlas.hcl double-quoted
		// string, where a Windows separator makes \U an invalid escape and the
		// whole project file is refused.
		liveURL:   "sqlite://" + filepath.ToSlash(livePath),
		modelsDir: modelsDir,
		sqlFile:   sqlFile,
		testsDir:  testsDir,
	}
}

// freshDevURL returns a throwaway database URL no other invocation has used.
//
// A shared --dev-url is not reset between invocations, so a negative source run
// against a database an earlier positive run populated reports "1 cases,
// 1 passed, 0 failed" and exit 0 -- the fixture would stop discriminating.
func freshDevURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
}

func runCompatArgs(args []string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// The four tests below cover every spelling that reaches the desired-state
// source, because the refusal they replace had three reachable gate branches
// with two different messages: -u and --url share the flag mapper, PTAH_URL is
// read before them, and an atlas.hcl env src goes through a different site
// altogether. Fixing one branch would look complete while leaving the others
// refusing, so each spelling is measured on its own.
//
// TestCompatCommand_SchemaTestFileSourcesStillResolveToThemselves is the
// control for all four: the asserted table exists only in the live database, so
// a source that resolved to anything else fails the same case.
func TestCompatCommand_SchemaTestFlagSpellingsAcceptADatabaseSource(t *testing.T) {
	tests := []struct {
		name string
		flag string
	}{
		{name: "-u shorthand with a database URL", flag: "-u"},
		{name: "--url long spelling with a database URL", flag: "--url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeCompatLiveSourceFixture(c)

			out, err := runCompatArgs([]string{
				"schema", "test", fixture.testsDir,
				tt.flag, fixture.liveURL, "--dev-url", freshDevURL(c),
			})

			assertDatabaseSourcePassed(c, out, err)
		})
	}
}

func TestCompatCommand_SchemaTestEnvironmentTwinAcceptsADatabaseSource(t *testing.T) {
	c := qt.New(t)
	fixture := writeCompatLiveSourceFixture(c)
	t.Setenv("PTAH_URL", fixture.liveURL)

	out, err := runCompatArgs([]string{"schema", "test", fixture.testsDir, "--dev-url", freshDevURL(c)})

	assertDatabaseSourcePassed(c, out, err)
}

func TestCompatCommand_SchemaTestProjectFileSrcAcceptsADatabaseSource(t *testing.T) {
	c := qt.New(t)
	fixture := writeCompatLiveSourceFixture(c)
	dir := c.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "local" {
  src = "`+fixture.liveURL+`"
  dev = "`+freshDevURL(c)+`"
}
`), 0o600), qt.IsNil)

	out, err := runCompatArgs([]string{"schema", "test", fixture.testsDir, "--env", "local"})

	assertDatabaseSourcePassed(c, out, err)
}

// TestCompatCommand_SchemaTestFileSourcesStillResolveToThemselves keeps the
// database source from becoming the answer to every -u: a file source still
// describes the schema in the file, which is the case the live-database tests
// above would pass even if the flag resolved to nothing at all.
func TestCompatCommand_SchemaTestFileSourcesStillResolveToThemselves(t *testing.T) {
	tests := []struct {
		name string
		// The path exists only once the fixture is written, so the row names
		// the field rather than the path. The closure asserts nothing.
		source func(fixture compatLiveSourceFixture) string
	}{
		{
			name:   "Go annotation directory",
			source: func(fixture compatLiveSourceFixture) string { return fixture.modelsDir },
		},
		{
			name:   "SQL schema file",
			source: func(fixture compatLiveSourceFixture) string { return fixture.sqlFile },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeCompatLiveSourceFixture(c)

			out, err := runCompatArgs([]string{
				"schema", "test", fixture.testsDir,
				"-u", "file://" + tt.source(fixture), "--dev-url", freshDevURL(c),
			})

			assertDatabaseSourceNotRead(c, out, err)
		})
	}
}

// TestCompatCommand_SchemaTestRefusesURLsThatAreNotADesiredState pins the two
// URL schemes that stay refused now that a database URL is accepted, each with
// its own reason. Neither refusal may fall back to the removed message about
// migration directories, which would send an operator to fix a flag that is not
// the problem.
func TestCompatCommand_SchemaTestRefusesURLsThatAreNotADesiredState(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "atlas:// registry URLs stay refused",
			url:  "atlas://myschema",
			want: "atlas:// registry URLs are not supported",
		},
		{
			name: "docker:// as desired state stays refused",
			url:  "docker://postgres/16/dev",
			want: "docker:// URLs provision Atlas dev databases and cannot be used as a desired-state source",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeCompatLiveSourceFixture(c)

			out, err := runCompatArgs([]string{"schema", "test", fixture.testsDir, "-u", tt.url})

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), "migration directories")
		})
	}
}

// assertDatabaseSourcePassed asserts the live database was actually read: the
// asserted table exists nowhere else.
func assertDatabaseSourcePassed(c *qt.C, out string, err error) {
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "db-sourced table exists"`)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// assertDatabaseSourceNotRead asserts the control sources still resolve to
// themselves. Exit status alone cannot carry this: a refused URL and a source
// that resolved to the wrong schema both exit 1, so the bytes decide.
func assertDatabaseSourceNotRead(c *qt.C, out string, err error) {
	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "no such table: orders_from_db")
}
