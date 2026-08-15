package migrationstest_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/migrationstest"
)

var errTestWrite = errors.New("test writer failed")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

func writeMigrationPair(tb testing.TB, dir, name, up, down string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
}

func runTestCommand(args ...string) (string, error) {
	cmd := migrationstest.NewMigrationsTestCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestMigrationsTestCommand_Passes(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	testsDir := t.TempDir()
	writeMigrationPair(c.TB, migrationsDir, "0000000001_create_users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", "DROP TABLE users;")
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users table works\n"+
			"    steps:\n"+
			"      - name: migrate\n"+
			"        migrate_to: latest\n"+
			"      - name: insert\n"+
			"        exec: INSERT INTO users (name) VALUES ('ada')\n"+
			"      - name: one user\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"1\"\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir, "--migrations-dir", migrationsDir, "--dir-format", "ptah")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "PASS  case \"users table works\"")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestMigrationsTestCommand_AppliesDesiredSchema(t *testing.T) {
	c := qt.New(t)
	modelsDir := t.TempDir()
	testsDir := t.TempDir()
	content := `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(content), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "schema.yaml"), []byte(
		"cases:\n"+
			"  - name: desired schema works\n"+
			"    steps:\n"+
			"      - name: apply\n"+
			"        apply_schema: true\n"+
			"      - name: table exists\n"+
			"        assert:\n"+
			"          query: SELECT id FROM users\n"+
			"          row_count: 0\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir, "--root-dir", modelsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "PASS  case \"desired schema works\"")
}

func TestMigrationsTestCommand_DefaultSeedDirectory(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	seedsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_users.test.sql"),
		[]byte("INSERT INTO users (name) VALUES ('ada');"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "seed.yaml"), []byte(
		"cases:\n"+
			"  - name: default seed directory works\n"+
			"    steps:\n"+
			"      - exec: CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)\n"+
			"      - seed:\n"+
			"          env: test\n"+
			"      - assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir, "--seed-dir", seedsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "default seed directory works"`)
}

func TestMigrationsTestCommand_ReportWriteFailure(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "pass.yaml"), []byte(
		"cases:\n"+
			"  - name: passing case\n"+
			"    steps:\n"+
			"      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	cmd := migrationstest.NewMigrationsTestCommand()
	cmd.SetOut(failingWriter{})
	cmd.SetArgs([]string{"--dir", testsDir})

	err := cmd.ExecuteContext(context.Background())

	c.Assert(err, qt.ErrorIs, errTestWrite)
}

func TestMigrationsTestCommand_FailsWithNonZeroError(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "fail.yaml"), []byte(
		"cases:\n"+
			"  - name: bad expectation\n"+
			"    steps:\n"+
			"      - name: create\n"+
			"        exec: CREATE TABLE t (id INTEGER PRIMARY KEY)\n"+
			"      - name: wrong count\n"+
			"        assert:\n"+
			"          query: SELECT id FROM t\n"+
			"          row_count: 5\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "migration tests failed")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "FAIL  case \"bad expectation\"")
}

func TestMigrationsTestCommand_NoCasesFound(t *testing.T) {
	c := qt.New(t)
	_, err := runTestCommand("--dir", t.TempDir())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no test cases found")
}

// TestMigrationsTestCommand_RefusesMissingMigrationsDirectory pins the failure
// a typo in --migrations-dir has to produce.
//
// `migrations test` reads a directory it never creates, so an absent one is a
// mistake, not an empty history. A snapshot that answered "no migrations" for a
// path that is not there would report a green `migrate_to: latest` step having
// executed nothing at all, and a suite whose only step is that one would pass
// while testing a directory the operator misspelled.
func TestMigrationsTestCommand_RefusesMissingMigrationsDirectory(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: migrations apply cleanly\n"+
			"    steps:\n"+
			"      - name: migrate\n"+
			"        migrate_to: latest\n"), 0o600), qt.IsNil)
	missingDir := filepath.Join(t.TempDir(), "migrations")

	out, err := runTestCommand("--dir", testsDir, "--migrations-dir", missingDir, "--dir-format", "ptah")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "read migration directory")
	c.Assert(err.Error(), qt.Contains, missingDir)
	c.Assert(out, qt.Not(qt.Contains), "1 passed")
}

func TestMigrationsTestCommand_RejectsUnsupportedReport(t *testing.T) {
	c := qt.New(t)
	_, err := runTestCommand("--dir", t.TempDir(), "--report", "xml")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unsupported report format")
}

func TestMigrationsTestCommand_RunPattern(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: selected case\n"+
			"    steps:\n"+
			"      - exec: SELECT 1\n"+
			"  - name: excluded case\n"+
			"    steps:\n"+
			"      - exec: SELECT missing_column\n"), 0o600), qt.IsNil)

	out, err := runTestCommand("--dir", testsDir, "--run", "^selected")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "selected case"`)
	c.Assert(out, qt.Not(qt.Contains), "excluded case")
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

func TestMigrationsTestCommand_RunPatternFailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := runTestCommand("--dir", t.TempDir(), "--run", "[")

	c.Assert(err, qt.ErrorMatches, `compile test case pattern "\[":.*`)
}

func TestMigrationsTestCommand_RunPatternNoMatches(t *testing.T) {
	c := qt.New(t)
	testsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: existing case\n"+
			"    steps:\n"+
			"      - exec: SELECT 1\n"), 0o600), qt.IsNil)

	_, err := runTestCommand("--dir", testsDir, "--run", "^missing$")

	c.Assert(err, qt.ErrorMatches, `no test cases match --run "\^missing\$"`)
}
