package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/dbschema"
)

func TestNewAtlasCommand_MigrateLintDevURLReplaysMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasLintDevURLFile(c, migrationsDir, "1_create_atlas_lint_dev_url.sql",
		"CREATE TABLE atlas_lint_dev_url (id INTEGER PRIMARY KEY);\n")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://" + devDBPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "No lint findings.")
	assertAtlasLintDevURLSQLiteTableCount(c, devDBPath, "atlas_lint_dev_url", 1)
}

func TestNewAtlasCommand_MigrateLintRejectsDockerDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	writeAtlasLintDevURLFile(c, migrationsDir, "1_create_atlas_lint_dev_url.sql",
		"CREATE TABLE atlas_lint_dev_url_docker (id INTEGER PRIMARY KEY);\n")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", migrationsDir,
		"--dev-url", "docker://postgres/16/dev",
	})

	err := cmd.Execute()

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(out.String(), qt.Contains, "docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for migration SQL replay")
}

func TestNewAtlasCommand_MigrateLintUsesAtlasProjectEnvPolicy(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeAtlasLintDevURLFile(c, migrationsDir, "1_drop_column.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeAtlasLintDevURLFile(c, migrationsDir, "1_drop_column.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    destructive {
      error = false
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "ci",
		"--format", "json",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"rule": "DS102"`)
	c.Assert(out.String(), qt.Contains, `"severity": "warning"`)
}

func writeAtlasLintDevURLFile(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func assertAtlasLintDevURLSQLiteTableCount(c *qt.C, dbPath, table string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
