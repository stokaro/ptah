package migraterepair_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migraterepair"
)

func TestMigrateRepairCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migraterepair.NewMigrateRepairCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "repair")
	c.Assert(cmd.Short, qt.Contains, "Repair dirty migration metadata")
	c.Assert(cmd.Flag("db-url"), qt.IsNotNil)
	c.Assert(cmd.Flag("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flag("version"), qt.IsNotNil)
	c.Assert(cmd.Flag("force"), qt.IsNotNil)
	c.Assert(cmd.Flag("resume-from"), qt.IsNotNil)
}
