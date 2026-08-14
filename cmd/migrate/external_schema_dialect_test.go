package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

const (
	sqlServerSchemaCommand = "go run ../internal/schemaops/testdata/sqlserver-schema-command"
	sqlServerDatabaseURL   = "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable"
)

func TestMigratePlan_UsesDatabaseDialectForExternalSQL(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", sqlServerSchemaCommand,
		"--db-url", sqlServerDatabaseURL,
		"--connect-timeout", "1ns",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`)
}

func TestMigrateGenerate_UsesDatabaseDialectForExternalSQL(t *testing.T) {
	c := qt.New(t)
	configPath := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(
		configPath,
		[]byte("url: "+sqlServerDatabaseURL+"\n"),
		0o600,
	), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", sqlServerSchemaCommand,
		"--config", configPath,
		"--migrations-dir", c.TempDir(),
		"--connect-timeout", "1ns",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`)
}

func TestMigratePlan_ValidatesConnectTimeoutBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
		"--connect-timeout", "invalid",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid --connect-timeout value "invalid": .*`)
}

func TestMigratePlan_ValidatesVirtualDropToggleBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)

	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
}

func TestMigratePlan_ValidatesSQLiteToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--db-url", "sqlite://test.db",
		"--config", configPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}

func TestMigratePlan_DoesNotApplySQLiteToggleBeforePostgresProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--db-url", "postgres://localhost/database",
		"--config", configPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config .*unknown ptah.yaml key "unknown".*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), sqlitevirtual.AllowDropEnvVar)
}

func TestMigrateGenerate_ValidatesConnectTimeoutBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
		"--migrations-dir", c.TempDir(),
		"--connect-timeout", "invalid",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid --connect-timeout value "invalid": .*`)
}

func TestMigrateGenerate_ValidatesVirtualDropToggleBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
		"--migrations-dir", c.TempDir(),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
}

func TestMigrateGenerate_ValidatesSQLiteToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--db-url", "sqlite://test.db",
		"--config", configPath,
		"--migrations-dir", c.TempDir(),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}

func TestMigrateGenerate_DoesNotApplySQLiteToggleBeforePostgresProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--db-url", "postgres://localhost/database",
		"--config", configPath,
		"--migrations-dir", c.TempDir(),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config .*unknown ptah.yaml key "unknown".*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), sqlitevirtual.AllowDropEnvVar)
}

func TestMigrateGenerateReplay_ValidatesSQLiteToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--replay",
		"--dev-url", "sqlite://dev.db",
		"--config", configPath,
		"--migrations-dir", c.TempDir(),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}

func TestMigrateGenerateReplay_DoesNotApplySQLiteToggleBeforePostgresProjectConfig(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--replay",
		"--dev-url", "postgres://localhost/dev",
		"--config", configPath,
		"--migrations-dir", c.TempDir(),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config .*unknown ptah.yaml key "unknown".*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), sqlitevirtual.AllowDropEnvVar)
}

func TestMigrateGenerate_ValidatesReportFormatBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
		"--migrations-dir", c.TempDir(),
		"--report", "xml",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported safety report format "xml"`)
}
