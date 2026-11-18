package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/dbcli"
)

// devURLCommand is a native command with a --dev-url flag and nothing else.
func devURLCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("dev-url", "", "")
	return cmd
}

func TestChecksUnusedDevURL_GivenForTheRun(t *testing.T) {
	t.Run("on the command line", func(t *testing.T) {
		c := qt.New(t)
		cmd := devURLCommand()
		c.Assert(cmd.Flags().Set("dev-url", "postgres://127.0.0.1:1/dev"), qt.IsNil)

		c.Assert(dbcli.ChecksUnusedDevURL(cmd, "dev-url"), qt.IsTrue)
	})
	t.Run("through its PTAH_ variable", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(cmdflags.EnvName("PTAH", "dev-url"), "postgres://127.0.0.1:1/dev")
		cmd := devURLCommand()
		c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

		c.Assert(dbcli.ChecksUnusedDevURL(cmd, "dev-url"), qt.IsTrue)
	})
}

// A value the command took from the project file never touched the flag, so
// the flag is all the helper sees; EffectiveString is the step that put the
// config value in place, and it is not this question.
func TestChecksUnusedDevURL_NotGivenForTheRun(t *testing.T) {
	t.Run("the flag is not set", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dbcli.ChecksUnusedDevURL(devURLCommand(), "dev-url"), qt.IsFalse)
	})
	t.Run("the command has no such flag", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dbcli.ChecksUnusedDevURL(devURLCommand(), "shadow-db"), qt.IsFalse)
	})
}
