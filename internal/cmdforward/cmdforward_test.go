package cmdforward_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cmdforward"
)

func TestRegister_HappyPath(t *testing.T) {
	c := qt.New(t)
	forwarder := &cobra.Command{Use: "down"}
	cmdforward.Register(forwarder, "migrations down", func() *cobra.Command {
		target := &cobra.Command{Use: "down"}
		target.Flags().String("target", "", "")
		return target
	})
	cmdforward.Refuse(forwarder, "confirm", "editor")
	cmdforward.ReadsPositionals(forwarder)

	c.Assert(cmdforward.Target(forwarder), qt.Equals, "migrations down")
	c.Assert(cmdforward.Refused(forwarder), qt.DeepEquals, []string{"confirm", "editor"})
	c.Assert(cmdforward.Positionals(forwarder), qt.IsTrue)
	built := cmdforward.Build(forwarder)
	c.Assert(built.Flags().Lookup("target"), qt.IsNotNil)
	// Each call builds a new command, as each run of the forwarder does.
	c.Assert(cmdforward.Build(forwarder), qt.Not(qt.Equals), built)
}

func TestRegister_FailurePath(t *testing.T) {
	c := qt.New(t)
	plain := &cobra.Command{Use: "plain"}
	cmdforward.Refuse(plain)
	c.Assert(cmdforward.Target(plain), qt.Equals, "")
	c.Assert(cmdforward.Refused(plain), qt.IsNil)
	c.Assert(cmdforward.Positionals(plain), qt.IsFalse)
	c.Assert(cmdforward.Build(plain), qt.IsNil)
}
