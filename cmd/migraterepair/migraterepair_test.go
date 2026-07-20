package migraterepair

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrateRepairCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := NewMigrateRepairCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "repair")
	c.Assert(cmd.Short, qt.Contains, "Repair dirty migration metadata")
	c.Assert(cmd.Flag(dbURLFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(migrationsFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(versionFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(forceFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(resumeFromFlag), qt.IsNotNil)
}

func TestParseResumeFrom(t *testing.T) {
	c := qt.New(t)

	resumeFrom, err := parseResumeFrom("")
	c.Assert(err, qt.IsNil)
	c.Assert(resumeFrom, qt.Equals, 0)

	resumeFrom, err = parseResumeFrom("3")
	c.Assert(err, qt.IsNil)
	c.Assert(resumeFrom, qt.Equals, 3)

	_, err = parseResumeFrom("0")
	c.Assert(err, qt.ErrorMatches, `invalid resume-from value "0"`)

	_, err = parseResumeFrom("bad")
	c.Assert(err, qt.ErrorMatches, `invalid resume-from value "bad"`)
}
