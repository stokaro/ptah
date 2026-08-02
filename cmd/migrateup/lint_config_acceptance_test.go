package migrateup_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	lintcmd "go.5x5.cz/ptah/cmd/lint"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/lint"
)

func TestMigrateUp_LintConfigSeverityOverrideControlsPostgresMigration(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL := requireLintGatePostgresURL(t)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tableName := "ptah_lint_warning_" + suffix
	revisionTable := "ptah_lint_warning_revisions_" + suffix
	defer dropLintGatePostgresTables(c, conn, tableName, revisionTable)

	dir := t.TempDir()
	writeLintGateMigration(c, dir, lint.ConfigFileName, `rules:
  DS103:
    severity: error
`)
	writeLintGateMigration(c, dir, "0000000001_init.up.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(255));\n", tableName))
	writeLintGateMigration(c, dir, "0000000001_init.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))
	writeLintGateMigration(c, dir, "0000000002_widen_email.up.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(512);\n", tableName))
	writeLintGateMigration(c, dir, "0000000002_widen_email.down.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(255);\n", tableName))

	var stdout, stderr bytes.Buffer
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--migrations-table", revisionTable,
	})

	err = cmd.Execute()
	c.Assert(err, qt.ErrorMatches, "(?s).*pending migrations contain destructive statements.*DS103.*error.*")

	writeLintGateMigration(c, dir, lint.ConfigFileName, `rules:
  DS103:
    severity: warning
`)
	stdout.Reset()
	stderr.Reset()
	cmd = migrateup.NewMigrateUpCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--migrations-table", revisionTable,
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	var maxLength int
	err = conn.QueryRowContext(ctx, `
		SELECT character_maximum_length
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'email'
	`, tableName).Scan(&maxLength)
	c.Assert(err, qt.IsNil)
	c.Assert(maxLength, qt.Equals, 512)
}

func TestMigrateUp_LintConfigWarningStillBlocksPostgresDropTable(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL := requireLintGatePostgresURL(t)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tableName := "ptah_lint_drop_" + suffix
	revisionTable := "ptah_lint_drop_revisions_" + suffix
	defer dropLintGatePostgresTables(c, conn, tableName, revisionTable)

	dir := t.TempDir()
	writeLintGateMigration(c, dir, lint.ConfigFileName, `rules:
  DS103:
    severity: warning
`)
	writeLintGateMigration(c, dir, "0000000001_init.up.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(255));\n", tableName))
	writeLintGateMigration(c, dir, "0000000001_init.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))
	writeLintGateMigration(c, dir, "0000000002_widen_email.up.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(512);\n", tableName))
	writeLintGateMigration(c, dir, "0000000002_widen_email.down.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(255);\n", tableName))
	writeLintGateMigration(c, dir, "0000000003_drop_table.up.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))
	writeLintGateMigration(c, dir, "0000000003_drop_table.down.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(512));\n", tableName))

	var stdout, stderr bytes.Buffer
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--migrations-table", revisionTable,
	})

	err = cmd.Execute()
	c.Assert(err, qt.ErrorMatches, "(?s).*pending migrations contain destructive statements.*DS101.*")

	var tableExists bool
	err = conn.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", "public."+tableName).Scan(&tableExists)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists, qt.IsFalse)
}

func TestMigrateUp_InvalidLintConfigFailsBeforeSQLiteMigrationExecution(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "ptah.db")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	writeLintGateMigration(c, migrationsDir, lint.ConfigFileName, "rules:\n  DS101:\n    severty: warning\n")
	writeLintGateMigration(c, migrationsDir, "0000000001_create.up.sql", "CREATE TABLE users (id INTEGER);\n")
	writeLintGateMigration(c, migrationsDir, "0000000001_create.down.sql", "DROP TABLE users;\n")

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

	assertLintAndMigrateExclusionParity(c, t, root, migrationsDir, "migrations", "migrations/legacy/**")
}

func TestMigrateUp_NestedDirectoryPrefixedExclusionMatchesLintCommand(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "db", "migrations")
	t.Chdir(root)

	assertLintAndMigrateExclusionParity(c, t, root, migrationsDir, "db/migrations", "db/migrations/legacy/**")
}

func TestMigrateUp_AbsoluteDirectoryPrefixedExclusionMatchesLintCommand(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	pattern := filepath.ToSlash(filepath.Join(migrationsDir, "legacy", "**"))

	assertLintAndMigrateExclusionParity(c, t, root, migrationsDir, migrationsDir, pattern)
}

func assertLintAndMigrateExclusionParity(
	c *qt.C,
	t *testing.T,
	root string,
	migrationsDir string,
	migrationsArg string,
	pattern string,
) {
	c.Helper()
	legacyDir := filepath.Join(migrationsDir, "legacy")
	c.Assert(os.MkdirAll(legacyDir, 0o755), qt.IsNil)
	writeLintGateMigration(c, migrationsDir, lint.ConfigFileName, fmt.Sprintf("rules:\n  DS101:\n    exclude:\n      - %q\n", pattern))
	writeLintGateMigration(c, legacyDir, "0000000001_create.up.sql", "CREATE TABLE users (id INTEGER);\n")
	writeLintGateMigration(c, legacyDir, "0000000001_create.down.sql", "DROP TABLE users;\n")
	writeLintGateMigration(c, legacyDir, "0000000002_drop.up.sql", "DROP TABLE users;\n")
	writeLintGateMigration(c, legacyDir, "0000000002_drop.down.sql", "CREATE TABLE users (id INTEGER);\n")

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

func requireLintGatePostgresURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	return ""
}

func writeLintGateMigration(c *qt.C, dir, name, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
}

func dropLintGatePostgresTables(c *qt.C, conn *dbschema.DatabaseConnection, names ...string) {
	c.Helper()
	for _, name := range names {
		statement, err := renderer.RenderSQL(platform.Postgres, ast.NewDropTable(name).SetIfExists().SetCascade())
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), statement)
		c.Check(err, qt.IsNil)
	}
}
