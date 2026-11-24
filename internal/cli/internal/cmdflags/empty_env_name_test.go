package cmdflags_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
)

// envCommand is a command with an --env that declares no rule of its own, so
// the binding reads an empty PTAH_ENV as unset and records that it did.
func envCommand(env *string) *cobra.Command {
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(env, "env", "", "Project env")
	return cmd
}

// TestEmptyEnvName_ReportsAnEmptyVariable is the one state the binding cannot
// tell from unset by the flag's value alone: the flag keeps its default, and
// the command asks here which of the two it was.
func TestEmptyEnvName_ReportsAnEmptyVariable(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_ENV", "")
	var env string
	cmd := envCommand(&env)

	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

	name, value, empty := cmdflags.EmptyEnvName(cmd.Flags(), "env")
	c.Assert(empty, qt.IsTrue)
	c.Assert(name, qt.Equals, "PTAH_ENV")
	c.Assert(value, qt.Equals, "")
	c.Assert(env, qt.Equals, "")
}

// TestEmptyEnvName_ReportsABlankVariableOnAFlagThatReadsBlankAsEmpty: on a
// flag declared with ReadBlankAsEmpty, a value of only white space is read as
// the empty one, and the value is reported.
func TestEmptyEnvName_ReportsABlankVariableOnAFlagThatReadsBlankAsEmpty(t *testing.T) {
	for _, blank := range []string{" ", "  ", "\t"} {
		t.Run(blank, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ENV", blank)
			var env string
			cmd := envCommand(&env)
			cmdflags.ReadBlankAsEmpty(cmd.Flags(), "env")

			c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

			name, value, empty := cmdflags.EmptyEnvName(cmd.Flags(), "env")
			c.Assert(empty, qt.IsTrue)
			c.Assert(name, qt.Equals, "PTAH_ENV")
			c.Assert(value, qt.Equals, blank)
			c.Assert(env, qt.Equals, "")
		})
	}
}

func TestEmptyEnvName_ReportsNothingElse(t *testing.T) {
	t.Run("an unset variable", func(t *testing.T) {
		c := qt.New(t)
		unsetEnv(c, "PTAH_ENV")
		var env string
		cmd := envCommand(&env)

		c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

		name, value, empty := cmdflags.EmptyEnvName(cmd.Flags(), "env")
		c.Assert(empty, qt.IsFalse)
		c.Assert(name, qt.Equals, "")
		c.Assert(value, qt.Equals, "")
	})

	tests := []struct {
		name  string
		value string
		args  []string
		flag  string
	}{
		{name: "a variable with a value", value: "dev", flag: "env"},
		{name: "a variable of only white space, which is a value on a flag that reads it so", value: " ", flag: "env"},
		{name: "an empty variable beside a typed --env", value: "", args: []string{"--env", "dev"}, flag: "env"},
		{name: "an empty variable beside a typed empty --env", value: "", args: []string{"--env", ""}, flag: "env"},
		{name: "a flag the set does not have", value: "", flag: "missing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ENV", test.value)
			var env string
			cmd := envCommand(&env)
			c.Assert(cmd.Flags().Parse(test.args), qt.IsNil)

			c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

			name, value, empty := cmdflags.EmptyEnvName(cmd.Flags(), test.flag)
			c.Assert(empty, qt.IsFalse)
			c.Assert(name, qt.Equals, "")
			c.Assert(value, qt.Equals, "")
		})
	}

	// A command tree is initialized once per run, and a test or an adapter
	// runs one tree twice. The record of an empty variable belongs to the run
	// that read it.
	t.Run("a later run that reads a value", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_ENV", "")
		var env string
		cmd := envCommand(&env)
		c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)
		c.Setenv("PTAH_ENV", "dev")

		c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

		name, value, empty := cmdflags.EmptyEnvName(cmd.Flags(), "env")
		c.Assert(empty, qt.IsFalse)
		c.Assert(name, qt.Equals, "")
		c.Assert(value, qt.Equals, "")
		c.Assert(env, qt.Equals, "dev")
	})
}
