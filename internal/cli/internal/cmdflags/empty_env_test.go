package cmdflags_test

import (
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/envbinding"
)

// schemaCommand is a command whose --schemas declares that its variable
// refuses a value naming no schema, beside a --label that declares nothing.
func schemaCommand(c *qt.C, schemas, label *string) *cobra.Command {
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(schemas, "schemas", "", "Schemas to read")
	cmd.Flags().StringVar(label, "label", "", "Label")
	rule := envbinding.EmptyRule{Omitted: "read every schema", List: true}
	c.Assert(cmdflags.RefuseEmptyEnv(cmd.Flags(), "schemas", rule), qt.IsNil)
	return cmd
}

// unsetEnv removes name for the rest of the test and restores it afterwards.
// Setenv registers the restore; an empty value would be a configuration of its
// own, so the variable is then removed.
func unsetEnv(c *qt.C, name string) {
	c.Setenv(name, "")
	c.Assert(os.Unsetenv(name), qt.IsNil)
}

func TestRefuseEmptyEnv_FailurePath(t *testing.T) {
	tests := []struct {
		value string
		state string
	}{
		{value: "", state: "set but empty"},
		{value: " ", state: `set to " ", which names nothing`},
		{value: ",", state: `set to ",", which names nothing`},
		{value: " , ", state: `set to " , ", which names nothing`},
	}

	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_SCHEMAS", test.value)
			var schemas, label string

			err := cmdflags.InitializeEnv("PTAH", schemaCommand(c, &schemas, &label))

			c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("PTAH_SCHEMAS is "+test.state+
				": give it a value, or unset it and leave --schemas out to read every schema"))
			c.Assert(schemas, qt.Equals, "")
		})
	}

	t.Run("a rule that names no default cannot be declared", func(t *testing.T) {
		c := qt.New(t)
		cmd := &cobra.Command{Use: "ptah"}
		cmd.Flags().String("schemas", "", "Schemas to read")

		err := cmdflags.RefuseEmptyEnv(cmd.Flags(), "schemas", envbinding.EmptyRule{List: true})

		c.Assert(err, qt.ErrorMatches, `--schemas: the rule names nothing that leaving the flag out does, so it would refuse nothing`)
	})

	t.Run("an unknown flag cannot be declared", func(t *testing.T) {
		c := qt.New(t)
		cmd := &cobra.Command{Use: "ptah"}

		err := cmdflags.RefuseEmptyEnv(cmd.Flags(), "schemas", envbinding.EmptyRule{Omitted: "read every schema"})

		c.Assert(err, qt.ErrorMatches, `flag "schemas" does not exist`)
	})
}

func TestRefuseEmptyEnv_HappyPath(t *testing.T) {
	t.Run("a value is applied", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_SCHEMAS", " app ,")
		var schemas, label string

		err := cmdflags.InitializeEnv("PTAH", schemaCommand(c, &schemas, &label))

		c.Assert(err, qt.IsNil)
		c.Assert(schemas, qt.Equals, " app ,")
	})

	t.Run("an unset variable leaves the flag at its default", func(t *testing.T) {
		c := qt.New(t)
		unsetEnv(c, "PTAH_SCHEMAS")
		var schemas, label string

		err := cmdflags.InitializeEnv("PTAH", schemaCommand(c, &schemas, &label))

		c.Assert(err, qt.IsNil)
		c.Assert(schemas, qt.Equals, "")
	})

	t.Run("the command line wins without reading the variable", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_SCHEMAS", " , ")
		var schemas, label string
		cmd := schemaCommand(c, &schemas, &label)
		c.Assert(cmd.Flags().Parse([]string{"--schemas", "app"}), qt.IsNil)

		err := cmdflags.InitializeEnv("PTAH", cmd)

		c.Assert(err, qt.IsNil)
		c.Assert(schemas, qt.Equals, "app")
	})

	// The control for the refusal: the declaration is per flag, so an empty
	// variable of a flag that does not make it still reads as unset.
	t.Run("an undeclared flag keeps reading an empty variable as unset", func(t *testing.T) {
		c := qt.New(t)
		unsetEnv(c, "PTAH_SCHEMAS")
		c.Setenv("PTAH_LABEL", "")
		var schemas, label string

		err := cmdflags.InitializeEnv("PTAH", schemaCommand(c, &schemas, &label))

		c.Assert(err, qt.IsNil)
		c.Assert(label, qt.Equals, "")
	})
}
