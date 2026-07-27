package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrate"
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
