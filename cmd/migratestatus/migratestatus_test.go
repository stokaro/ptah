package migratestatus_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migratestatus"
)

func TestMigrateStatusCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migratestatus.NewMigrateStatusCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "status")
	c.Assert(cmd.Short, qt.Contains, "Show current migration status")
	c.Assert(cmd.Flag(dbcli.ConfigFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(dbcli.MigrationsSchemaFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(dbcli.MigrationsTableFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag("exit-code"), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.LogFormatFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.LogLevelFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.MetricsAddrFlagName), qt.IsNotNil)
}

func TestMigrateStatusCommand_UnreachableDatabaseExits2(t *testing.T) {
	c := qt.New(t)

	cmd := migratestatus.NewMigrateStatusCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"--db-url", "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"--migrations-dir", filepath.ToSlash(t.TempDir()),
		"--connect-timeout", "1ms",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(errOut.String(), qt.Contains, "error connecting to database")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "Usage:")
}

func TestMigrateStatusCommand_ExplicitEmptyAtlasDirBeatsBuiltinDefault(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	ptahConfigPath := filepath.Join(dir, "ptah.yaml")
	atlasConfigPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(
		ptahConfigPath,
		[]byte("migration:\n  dir: ptah-migrations\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		atlasConfigPath,
		[]byte(`env "local" {
  migration {
    dir = ""
  }
}
`),
		0o600,
	), qt.IsNil)

	cmd := migratestatus.NewMigrateStatusCommand()
	migrationsFlag := cmd.Flag("migrations-dir")
	c.Assert(migrationsFlag, qt.IsNotNil)
	c.Assert(migrationsFlag.Value.Set("migrations"), qt.IsNil)
	migrationsFlag.DefValue = "migrations"
	c.Assert(migrationsFlag.Changed, qt.IsFalse)

	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + filepath.Join(dir, "status.db"),
		"--config", ptahConfigPath,
		"--atlas-project-config", atlasConfigPath,
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "migrations directory is required")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(errOut.String(), qt.Equals, "error: migrations directory is required\n")
}
