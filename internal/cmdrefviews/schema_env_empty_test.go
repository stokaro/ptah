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

// writeVariableRefusals is every flag outside schemaVariables whose variable
// refuses a value that names nothing, by command path and flag name. Each
// command writes, and its variable read as unset would select more than any
// value does: an empty PTAH_EXCLUDE plans the DROP a value keeps out, and an
// empty PTAH_TO_VERSION applies every pending migration. A command that only
// reads, or one that writes a plan for review, keeps reading such a value as
// unset. The list is compared with both trees in both directions, so a new
// refusal is a decision made here, and a refusal dropped from a listed flag
// fails too. The table under "Variables that narrow a write" in
// docs/site/src/content/docs/reference/configuration.mdx names the same
// commands; change both together.
var writeVariableRefusals = []string{
	"ptah migrations data --protected-table",
	"ptah migrations down --limit",
	"ptah migrations down --target",
	"ptah migrations generate --exclude",
	"ptah migrations generate --include",
	"ptah migrations up --limit",
	"ptah migrations up --to-version",
	"ptah schema apply --exclude",
	"ptah schema apply --include",
	"ptah schema apply --protected-table",
	"ptah seed --protected-table",
	"ptah-compat migrate apply --to-version",
	"ptah-compat schema apply --exclude",
	"ptah-compat schema apply --include",
	"ptah-compat schema clean --exclude",
	"ptah-compat schema clean --include",
}

// emptyRefusingBindings walks tree and returns every bound flag outside
// schemaVariables and historyVariables that declares an empty rule.
func emptyRefusingBindings(tree *cobra.Command) []boundFlag {
	var found []boundFlag
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
			variable, bound := envbinding.Of("PTAH", flag)
			_, refused := envbinding.EmptyRefused(flag)
			if bound && refused && !slices.Contains(schemaVariables, variable) && !slices.Contains(historyVariables, variable) {
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

// TestWriteVariableRefusalsAreTheDeclaredOnes holds the refusal of an empty
// write variable to writeVariableRefusals on both binaries, and asks each rule
// to refuse the empty value, so a declaration that refuses nothing does not
// pass for one in effect.
func TestWriteVariableRefusalsAreTheDeclaredOnes(t *testing.T) {
	c := qt.New(t)
	bindings := slices.Concat(
		emptyRefusingBindings(root.NewRootCommand()),
		emptyRefusingBindings(cmdrefviews.Compat(atlascompatpolicy.Full())),
	)

	found := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		found = append(found, binding.command+" --"+binding.flag.Name)
		rule, _ := envbinding.EmptyRefused(binding.flag)
		comment := qt.Commentf("%s --%s reads %s", binding.command, binding.flag.Name, binding.variable)
		c.Check(rule.Check(binding.variable, binding.flag.Name, ""), qt.ErrorIs, envbinding.ErrEmptyValue, comment)
	}
	slices.Sort(found)
	c.Assert(found, qt.DeepEquals, writeVariableRefusals)
}

// historyVariables are the variables that say where the history of applied
// migrations is: the revision table, its schema and its layout. Read as unset,
// an empty one names the default history instead of the one the operator
// meant, and a run over it finds nothing applied.
var historyVariables = []string{
	"PTAH_MIGRATIONS_SCHEMA",
	"PTAH_MIGRATIONS_TABLE",
	"PTAH_REVISIONS_SCHEMA",
	"PTAH_REVISION_FORMAT",
}

// historyVariablesReadAsUnset are the bindings of historyVariables that keep
// reading an empty value as unset, by command path and flag name. `migrations
// lint` reads no history: the format only decides whether a version that is
// not a number is reported, and its default, ptah, reports it.
var historyVariablesReadAsUnset = []string{
	"ptah migrations lint --revision-format",
}

// minHistoryVariableBindings is a floor on the bindings the walk finds, so a
// walk that stopped matching cannot pass by finding nothing. The native tree
// binds the variables on 41 flags that refuse, and the compatibility tree
// PTAH_REVISIONS_SCHEMA on 5.
const minHistoryVariableBindings = 46

// historyVariableBindings walks tree and returns every flag bound to one of
// historyVariables, split into those whose variable refuses an empty value and
// the command paths and flag names of those that read it as unset.
func historyVariableBindings(tree *cobra.Command) (refusing []boundFlag, readAsUnset []string) {
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
			variable, bound := envbinding.Of("PTAH", flag)
			if !bound || !slices.Contains(historyVariables, variable) {
				return
			}
			if _, refused := envbinding.EmptyRefused(flag); refused {
				refusing = append(refusing, boundFlag{command: cmd.CommandPath(), variable: variable, flag: flag})
				return
			}
			readAsUnset = append(readAsUnset, cmd.CommandPath()+" --"+flag.Name)
		})
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(tree)
	return refusing, readAsUnset
}

// TestEveryHistoryVariableRefusesAnEmptyValue holds every binding of
// historyVariables on both binaries to the refusal, except the ones
// historyVariablesReadAsUnset names, so a flag added without it fails. Each
// rule is asked to refuse the empty value too, so a declaration that refuses
// nothing does not pass for one in effect.
func TestEveryHistoryVariableRefusesAnEmptyValue(t *testing.T) {
	c := qt.New(t)
	nativeRefusing, nativeUnset := historyVariableBindings(root.NewRootCommand())
	compatRefusing, compatUnset := historyVariableBindings(cmdrefviews.Compat(atlascompatpolicy.Full()))
	refusing := slices.Concat(nativeRefusing, compatRefusing)

	c.Assert(len(refusing) >= minHistoryVariableBindings, qt.IsTrue, qt.Commentf("found %d bindings", len(refusing)))
	c.Assert(slices.Concat(nativeUnset, compatUnset), qt.DeepEquals, historyVariablesReadAsUnset)
	for _, binding := range refusing {
		rule, _ := envbinding.EmptyRefused(binding.flag)
		comment := qt.Commentf("%s --%s reads %s", binding.command, binding.flag.Name, binding.variable)
		c.Check(rule.Check(binding.variable, binding.flag.Name, ""), qt.ErrorIs, envbinding.ErrEmptyValue, comment)
		c.Check(rule.Check(binding.variable, binding.flag.Name, " "), qt.ErrorIs, envbinding.ErrEmptyValue, comment)
	}
}
