package migratestatus_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migratestatus"
)

func TestMigrateStatusCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migratestatus.NewMigrateStatusCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "migrate-status")
	c.Assert(cmd.Short, qt.Contains, "Show current migration status")
	c.Assert(cmd.Flag(dbcli.ConfigFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(dbcli.MigrationsSchemaFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(dbcli.MigrationsTableFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag("exit-code"), qt.IsNotNil)
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
