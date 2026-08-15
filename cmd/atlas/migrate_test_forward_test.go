package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// writeMigrateTestFixture fills migrationsDir with an Atlas-format migration
// and testsDir with a passing Ptah-native YAML test case.
func writeMigrateTestFixture(tb testing.TB, migrationsDir, testsDir string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users table accepts rows\n"+
			"    steps:\n"+
			"      - name: migrate to latest\n"+
			"        migrate_to: latest\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - name: one user\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"1\"\n"), 0o600), qt.IsNil)
}

func TestCompatCommand_MigrateTestForwardsToNative(t *testing.T) {
	c := qt.New(t)
	migrationsDir, testsDir := t.TempDir(), t.TempDir()
	writeMigrateTestFixture(c.TB, migrationsDir, testsDir)
	devDB := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "test", testsDir,
		"--dir", "file://" + migrationsDir,
		"--dev-url", devDB,
	})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah migrations test`: --dir maps to the
	// native --migrations-dir (read as an Atlas-format directory), --dev-url to
	// the native throwaway --db-url, and the positional path to the native
	// test-case --dir.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users table accepts rows"`)
	c.Assert(out.String(), qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestCompatCommand_MigrateTestNativeDirectoryEnvironmentOverridesAtlasDefault(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	migrationsDir, testsDir := t.TempDir(), t.TempDir()
	writeMigrateTestFixture(c.TB, migrationsDir, testsDir)
	t.Setenv("PTAH_MIGRATIONS_DIR", migrationsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "test", testsDir,
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "users table accepts rows"`)
}

func TestCompatCommand_MigrateTestFailingCaseExits1(t *testing.T) {
	c := qt.New(t)
	migrationsDir, testsDir := t.TempDir(), t.TempDir()
	writeMigrateTestFixture(c.TB, migrationsDir, testsDir)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: failing expectation\n"+
			"    steps:\n"+
			"      - migrate_to: latest\n"+
			"      - assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"42\"\n"), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "test", testsDir, "--dir", "file://" + migrationsDir})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "migration tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out.String(), qt.Contains, `FAIL  case "failing expectation"`)
}

func TestCompatCommand_MigrateTestRunFilterSelectsCases(t *testing.T) {
	c := qt.New(t)
	migrationsDir, testsDir := t.TempDir(), t.TempDir()
	writeMigrateTestFixture(c.TB, migrationsDir, testsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "test", testsDir,
		"--dir", "file://" + migrationsDir,
		"--run", "^does-not-match$",
	})

	err := cmd.Execute()

	// The Atlas --run filter forwards to the native --run pattern; a pattern
	// with no matches fails loudly instead of reporting an empty pass.
	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^does-not-match\$"`)
}

func TestCompatCommand_MigrateTestRejectsUnsupportedDirFormat(t *testing.T) {
	c := qt.New(t)
	migrationsDir, testsDir := t.TempDir(), t.TempDir()
	writeMigrateTestFixture(c.TB, migrationsDir, testsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "test", testsDir,
		"--dir", "file://" + migrationsDir,
		"--dir-format", "goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`atlas migrate test --dir-format: Atlas accepts --dir-format=goose, but Ptah does not implement that directory format yet`)
}

func TestCompatCommand_MigrateTestRejectsMultiplePaths(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "test", "tests-a", "tests-b"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate test accepts one paths argument, got \["tests-a" "tests-b"\]`)
}

func TestCompatCommand_MigrateTestUsesAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: project config works\n"+
			"    steps:\n"+
			"      - migrate_to: latest\n"+
			"      - assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          row_count: 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "dev.db"))+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "test", testsDir, "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `PASS  case "project config works"`)
}

func TestNewCompatCommand_MigrateTestResolvesAtRoot(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "test", "--help"})

	err := cmd.Execute()

	// The verb resolves as a working forward through the compatibility binary.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas migrate test [flags] [paths]")
	c.Assert(out.String(), qt.Contains, "--dev-url")
}
