package cmdrefviews_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/cmdrefviews"
	"ptah.run/internal/envbinding"
)

// schemaVariables are the variables that select schemas. Set but empty, each
// would otherwise read as unset and widen the run to what leaving the flag out
// selects.
var schemaVariables = []string{"PTAH_SCHEMA", "PTAH_SCHEMAS"}

// minSchemaVariableBindings is a floor on the bindings the walk finds, so a
// walk that stopped matching cannot pass by finding nothing. Both trees bind
// one of the variables on 28 flags.
const minSchemaVariableBindings = 28

// boundFlag is one flag of one command and the variable it reads.
type boundFlag struct {
	command  string
	variable string
	flag     *pflag.Flag
}

// schemaVariableBindings walks tree and returns every flag bound to one of
// schemaVariables.
func schemaVariableBindings(tree *cobra.Command) []boundFlag {
	var found []boundFlag
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
			variable, bound := envbinding.Of("PTAH", flag)
			if bound && slices.Contains(schemaVariables, variable) {
				found = append(found, boundFlag{command: cmd.CommandPath(), variable: variable, flag: flag})
			}
		})
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(tree)
	return found
}

// TestEverySchemaVariableRefusesAnEmptyValue holds the declaration to every
// binding of PTAH_SCHEMA and PTAH_SCHEMAS on both binaries: a flag added
// without it would read the empty variable as unset. The rule each flag
// declares is asked to refuse the empty value too, so a declaration that
// records a rule refusing nothing does not pass for one in effect.
func TestEverySchemaVariableRefusesAnEmptyValue(t *testing.T) {
	c := qt.New(t)
	bindings := slices.Concat(
		schemaVariableBindings(root.NewRootCommand()),
		schemaVariableBindings(cmdrefviews.Compat(atlascompatpolicy.Full())),
	)

	c.Assert(len(bindings) >= minSchemaVariableBindings, qt.IsTrue, qt.Commentf("found %d bindings", len(bindings)))
	for _, binding := range bindings {
		rule, refused := envbinding.EmptyRefused(binding.flag)
		comment := qt.Commentf("%s --%s reads %s", binding.command, binding.flag.Name, binding.variable)
		c.Check(refused, qt.IsTrue, comment)
		c.Check(rule.Check(binding.variable, binding.flag.Name, ""), qt.ErrorIs, envbinding.ErrEmptyValue, comment)
	}
}
