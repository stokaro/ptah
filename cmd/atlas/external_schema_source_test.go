package atlas_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// seedSQLiteDBAt creates a SQLite database with the given DDL at path.
func seedSQLiteDBAt(t *testing.T, path, ddl string) {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), ddl)
	c.Assert(err, qt.IsNil)
}

// externalSchemaHelperModes maps a mode name to the behavior the re-executed
// test binary performs when it stands in for an atlas.hcl data.external_schema
// program.
var externalSchemaHelperModes = map[string]func(){
	"sql": func() {
		fmt.Fprint(os.Stdout, "CREATE TABLE ext_users (\n  id INTEGER PRIMARY KEY,\n  email TEXT NOT NULL\n);\n")
		os.Exit(0)
	},
	"fail": func() {
		fmt.Fprintln(os.Stderr, "external loader blew up")
		os.Exit(3)
	},
	"empty": func() {
		os.Exit(0)
	},
}

// TestExternalSchemaHelperProcess is not a real test; the tests below
// re-execute this binary with -test.run=TestExternalSchemaHelperProcess to act
// as the atlas.hcl-declared external schema program.
func TestExternalSchemaHelperProcess(t *testing.T) {
	runExternalSchemaHelperProcess()
}

func runExternalSchemaHelperProcess() {
	if os.Getenv("GO_WANT_ATLAS_EXTERNAL_HELPER") != "1" {
		return
	}
	emit, ok := externalSchemaHelperModes[os.Getenv("ATLAS_EXTERNAL_HELPER_MODE")]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown helper mode")
		os.Exit(1)
	}
	emit()
}

// writeExternalSchemaAtlasHCL writes an atlas.hcl declaring a
// data.external_schema source backed by this test binary and returns the
// config path.
func writeExternalSchemaAtlasHCL(t *testing.T, mode string) string {
	t.Helper()
	c := qt.New(t)
	baseDir := t.TempDir()
	configPath := filepath.Join(baseDir, "atlas.hcl")
	config := fmt.Sprintf(`data "external_schema" "app" {
  program = [%s, "-test.run=TestExternalSchemaHelperProcess"]
  env     = ["GO_WANT_ATLAS_EXTERNAL_HELPER=1", "ATLAS_EXTERNAL_HELPER_MODE=%s", "GORACE=atexit_sleep_ms=0"]
}

env "dev" {
  src = data.external_schema.app.url
}
`, strconv.Quote(os.Args[0]), mode)
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil) // #nosec -- controlled test-only path
	return configPath
}

func runCompatCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestSchemaDiffExternalSchemaSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedSQLiteDBAt(t, targetPath, "CREATE TABLE existing (id INTEGER PRIMARY KEY)")

	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "CREATE TABLE")
	c.Assert(out, qt.Contains, "ext_users")
}

func TestSchemaDiffRejectsMalformedSQLiteVirtualDropToggleBeforeExternalSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := writeExternalSchemaAtlasHCL(t, "fail")
	targetPath := filepath.Join(t.TempDir(), "target.db")

	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "external loader blew up")
}

func TestSchemaDiffExternalSchemaGate_FailurePath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset("PTAH_ALLOW_EXTERNAL_SCHEMA")(t)
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedSQLiteDBAt(t, targetPath, "CREATE TABLE existing (id INTEGER PRIMARY KEY)")

	_, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "atlas.hcl data.external_schema executes a repository-controlled program and is disabled by default")
	c.Assert(err.Error(), qt.Contains, "PTAH_ALLOW_EXTERNAL_SCHEMA=1")
}

func TestSchemaApplyExternalSchemaSourceDryRun(t *testing.T) {
	allowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	targetPath := filepath.Join(t.TempDir(), "target.db")

	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
		// --auto-approve is deliberately absent: the pair is refused, by both
		// binaries, and a dry run has nothing to approve (stokaro/ptah#1231
		// case 5).
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "CREATE TABLE")
	c.Assert(out, qt.Contains, "ext_users")
	c.Assert(sqliteHasTable(t, targetPath, "ext_users"), qt.IsFalse)
}

func TestSchemaApplyRejectsMalformedSQLiteVirtualDropToggleBeforeExternalSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := writeExternalSchemaAtlasHCL(t, "fail")
	targetPath := filepath.Join(t.TempDir(), "target.db")

	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "external loader blew up")
}

func TestSchemaApplyExternalSchemaSourceApplies(t *testing.T) {
	allowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	targetPath := filepath.Join(t.TempDir(), "target.db")

	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteHasTable(t, targetPath, "ext_users"), qt.IsTrue)
}

func TestSchemaInspectExternalSchemaSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runCompatCommand(t,
		"schema", "inspect",
		"--url", "env://src",
		"--dev-url", "sqlite://"+devPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "ext_users")
}

func TestMigrateDiffExternalSchemaSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")

	out, err := runCompatCommand(t,
		"migrate", "diff", "add_ext_users",
		"--config", "file://"+configPath,
		"--env", "dev",
		"--dev-url", "sqlite://"+devPath,
		"--dir", "file://"+migrationsDir,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "CREATE TABLE")
	c.Assert(out, qt.Contains, "ext_users")
}

func TestSchemaPlanExternalSchemaSource_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")

	_, err := runCompatCommand(t,
		"schema", "plan",
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "atlas schema plan does not support atlas.hcl data.external_schema desired state yet; pass --to explicitly")
}

func TestSchemaTestExternalSchemaSource_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "sql")

	_, err := runCompatCommand(t,
		"schema", "test",
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "atlas schema test does not support atlas.hcl data.external_schema desired state yet; pass --url explicitly")
}

func TestSchemaDiffExternalSchemaProgramFailure(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "fail")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedSQLiteDBAt(t, targetPath, "CREATE TABLE existing (id INTEGER PRIMARY KEY)")

	_, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "external loader blew up")
}

func TestSchemaDiffExternalSchemaEmptyOutput(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
	configPath := writeExternalSchemaAtlasHCL(t, "empty")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedSQLiteDBAt(t, targetPath, "CREATE TABLE existing (id INTEGER PRIMARY KEY)")

	_, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+targetPath,
		"--config", "file://"+configPath,
		"--env", "dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "produced empty output")
}
