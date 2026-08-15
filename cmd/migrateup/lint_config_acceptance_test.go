package migrateup_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	lintcmd "go.5x5.cz/ptah/cmd/lint"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/lint"
)

func TestMigrateUp_InvalidLintConfigFailsBeforeSQLiteMigrationExecution(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "ptah.db")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	writeLintGateMigration(c.TB, migrationsDir, lint.ConfigFileName, "rules:\n  DS101:\n    severty: warning\n")
	writeLintGateMigration(c.TB, migrationsDir, "0000000001_create.up.sql", "CREATE TABLE users (id INTEGER);\n")
	writeLintGateMigration(c.TB, migrationsDir, "0000000001_create.down.sql", "DROP TABLE users;\n")

	var stdout, stderr bytes.Buffer
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
	})

	err := cmd.Execute()
	c.Assert(err, qt.ErrorMatches, `(?s).*failed to parse lint config .*field severty not found in type lint.RuleConfig.*`)

	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var usersTableCount int
	err = conn.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'users'",
	).Scan(&usersTableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(usersTableCount, qt.Equals, 0)
}

func TestMigrateUp_RelativeDirectoryPrefixedExclusionMatchesLintCommand(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	t.Chdir(root)

	assertLintAndMigrateExclusionParity(c.TB, t, root, migrationsDir, "migrations", "migrations/legacy/**")
}

func TestMigrateUp_NestedDirectoryPrefixedExclusionMatchesLintCommand(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "db", "migrations")
	t.Chdir(root)

	assertLintAndMigrateExclusionParity(c.TB, t, root, migrationsDir, "db/migrations", "db/migrations/legacy/**")
}

func TestMigrateUp_AbsoluteDirectoryPrefixedExclusionMatchesLintCommand(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	pattern := filepath.ToSlash(filepath.Join(migrationsDir, "legacy", "**"))

	assertLintAndMigrateExclusionParity(c.TB, t, root, migrationsDir, migrationsDir, pattern)
}

func assertLintAndMigrateExclusionParity(
	tb testing.TB,
	t *testing.T,
	root string,
	migrationsDir string,
	migrationsArg string,
	pattern string,
) {
	c := qt.New(tb)
	c.Helper()
	legacyDir := filepath.Join(migrationsDir, "legacy")
	c.Assert(os.MkdirAll(legacyDir, 0o755), qt.IsNil)
	writeLintGateMigration(c.TB, migrationsDir, lint.ConfigFileName, fmt.Sprintf("rules:\n  DS101:\n    exclude:\n      - %q\n", pattern))
	writeLintGateMigration(c.TB, legacyDir, "0000000001_create.up.sql", "CREATE TABLE users (id INTEGER);\n")
	writeLintGateMigration(c.TB, legacyDir, "0000000001_create.down.sql", "DROP TABLE users;\n")
	writeLintGateMigration(c.TB, legacyDir, "0000000002_drop.up.sql", "DROP TABLE users;\n")
	writeLintGateMigration(c.TB, legacyDir, "0000000002_drop.down.sql", "CREATE TABLE users (id INTEGER);\n")

	var lintStdout, lintStderr bytes.Buffer
	lintCommand := lintcmd.NewLintCommand()
	lintCommand.SetOut(&lintStdout)
	lintCommand.SetErr(&lintStderr)
	lintCommand.SetArgs([]string{"--dir", migrationsArg})

	err := lintCommand.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", lintStderr.String()))
	c.Assert(lintStdout.String(), qt.Contains, "No lint findings")

	dbURL := "sqlite://" + filepath.Join(root, "ptah.db")
	var upStdout, upStderr bytes.Buffer
	upCommand := migrateup.NewMigrateUpCommand()
	upCommand.SetOut(&upStdout)
	upCommand.SetErr(&upStderr)
	upCommand.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", migrationsArg,
	})

	err = upCommand.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", upStderr.String()))

	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var usersTableCount int
	err = conn.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'users'",
	).Scan(&usersTableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(usersTableCount, qt.Equals, 0)
	var currentVersion int64
	err = conn.QueryRowContext(t.Context(), "SELECT MAX(version) FROM schema_migrations").Scan(&currentVersion)
	c.Assert(err, qt.IsNil)
	c.Assert(currentVersion, qt.Equals, int64(2))
}

func writeLintGateMigration(tb testing.TB, dir, name, contents string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
}
