package migratebaseline_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migratebaseline"
)

func TestMigrateBaselineCommandCreation(t *testing.T) {
	c := qt.New(t)

	cmd := migratebaseline.NewMigrateBaselineCommand()
	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "baseline")
	c.Assert(cmd.Flag("db-url"), qt.IsNotNil)
	c.Assert(cmd.Flag("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flag("version"), qt.IsNotNil)
	c.Assert(cmd.Flag("force"), qt.IsNotNil)
	c.Assert(cmd.Flag("dry-run"), qt.IsNotNil)
	c.Assert(cmd.Flag("shadow-db"), qt.IsNotNil)
	c.Assert(cmd.Flag("dir-format"), qt.IsNotNil)
	c.Assert(cmd.Flag("migration-lock-timeout"), qt.IsNotNil)
}
