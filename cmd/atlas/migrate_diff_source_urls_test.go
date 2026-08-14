package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestMigrateDiffDatabaseURLDesiredState(t *testing.T) {
	c := qt.New(t)
	desiredPath := seedSQLiteDB(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_desired_users",
		"--to", "sqlite://" + desiredPath,
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "desired_users")
	c.Assert(sqliteHasTable(t, desiredPath, "desired_users"), qt.IsTrue)
	entries, readErr := os.ReadDir(migrationsDir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func TestMigrateDiffRejectsMalformedSQLiteVirtualDropToggleBeforeSourceAndConnect(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	devPath := filepath.Join(t.TempDir(), "missing", "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "toggle_order",
		"--to", "file://" + filepath.Join(t.TempDir(), "missing.sql"),
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --dev-url")
	c.Assert(out.String(), qt.Not(qt.Contains), "missing.sql")
	_, statErr := os.Stat(migrationsDir)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestMigrateDiffEnvSrcDesiredState(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "schema_file" {}

env "dev" {
  src = "file://${var.schema_file}"
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(baseDir, "desired.sql"), []byte(`
CREATE TABLE env_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_env_users",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "schema_file=desired.sql",
		"--to", "env://src",
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "env_users")
}

func TestMigrateDiffConfigDefaultCompositeLocalDesiredState(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "dev_url" {}

env "dev" {
  dev = var.dev_url
  schema {
    src = ["file://accounts.sql", "file://orders.sql"]
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(baseDir, "accounts.sql"),
		[]byte("CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(baseDir, "orders.sql"),
		[]byte("CREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_composite_schema",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "dev_url=sqlite://" + devPath,
		"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `CREATE TABLE "accounts"`)
	c.Assert(out.String(), qt.Contains, `CREATE TABLE "orders"`)
}

func TestMigrateDiffEnvURLDesiredState(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	desiredPath := seedSQLiteDB(t, "CREATE TABLE env_database_users (id INTEGER PRIMARY KEY)")
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "desired_url" {}

env "dev" {
  url = var.desired_url
}
`), 0o600), qt.IsNil)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_env_database_users",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "desired_url=sqlite://" + desiredPath,
		"--to", "env://url",
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "env_database_users")
}

func TestMigrateDiffConfigDefaultDatabaseDesiredState(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	desiredPath := seedSQLiteDB(t, "CREATE TABLE config_database_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(baseDir, "migrations")
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "desired_url" {}
variable "dev_url" {}

env "dev" {
  dev = var.dev_url
  schema {
    src = var.desired_url
  }
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_config_database_users",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "desired_url=sqlite://" + desiredPath,
		"--var", "dev_url=sqlite://" + devPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "config_database_users")
	c.Assert(sqliteHasTable(t, desiredPath, "config_database_users"), qt.IsTrue)
	entries, readErr := os.ReadDir(migrationsDir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func TestMigrateDiffMigrationDirectoryDesiredState(t *testing.T) {
	c := qt.New(t)
	desiredDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(desiredDir, "1_create_users.sql"),
		[]byte("CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(desiredDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "import_replayed_users",
		"--to", "file://" + desiredDir,
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "replayed_users")
}

func TestMigrateDiffAliasedDesiredDatabaseFailsWithoutMutation(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	databasePath := seedSQLiteDB(t, "CREATE TABLE protected_users (id INTEGER PRIMARY KEY)")
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "database_url" {}

env "dev" {
  dev = var.database_url
}
`), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "database_url=sqlite://" + databasePath,
		"--to", "env://dev",
		"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`--to database must differ from --dev-url because the dev database is reset during planning`)
	c.Assert(sqliteHasTable(t, databasePath, "protected_users"), qt.IsTrue)
}

func TestMigrateDiffSourceFailureDoesNotCreateMigrationDirectory(t *testing.T) {
	c := qt.New(t)
	missingSourcePath := filepath.Join(t.TempDir(), "missing", "source.db")
	devPath := filepath.Join(t.TempDir(), "dev.db")
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--to", "sqlite://" + missingSourcePath,
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + migrationsDir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `load --to schema: connect to --to database: .*`)
	_, statErr := os.Stat(migrationsDir)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestMigrateDiffDatabaseURLDialectMismatchFailsBeforeDevConnection(t *testing.T) {
	c := qt.New(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--to", "postgres://localhost/desired",
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--to database dialect "postgres" does not match --dev-url dialect "sqlite"`)
	_, statErr := os.Stat(devPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestMigrateDiffEnvSourceWithoutConfigFailsBeforeDevConnection(t *testing.T) {
	c := qt.New(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--to", "env://src",
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`--to "env://src": env:// desired-state references require an evaluated atlas.hcl project configuration; pass --config and --env to select one`)
	_, statErr := os.Stat(devPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
