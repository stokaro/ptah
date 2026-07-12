package migratestatus_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/migratestatus"
)

func TestMigrateStatusCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migratestatus.NewMigrateStatusCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "migrate-status")
	c.Assert(cmd.Short, qt.Contains, "Show current migration status")
	c.Assert(cmd.Flag(dbcli.MigrationsSchemaFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(dbcli.MigrationsTableFlagName), qt.IsNotNil)
}
