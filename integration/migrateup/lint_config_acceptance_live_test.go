//go:build integration

package migrateup_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

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

func dropLintGatePostgresTables(c *qt.C, conn *dbschema.DatabaseConnection, names ...string) {
	c.Helper()
	for _, name := range names {
		statement, err := renderer.RenderSQL(platform.Postgres, ast.NewDropTable(name).SetIfExists().SetCascade())
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), statement)
		c.Check(err, qt.IsNil)
	}
}
