package cmdrefviews_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/cmdrefviews"
	"ptah.run/internal/envbinding"
)

// lockEngineVariables are the variables that name the lock a migration run
// takes and the engine of the history table it creates. Set but empty, each
// reads as unset: the run takes the default lock, which a runner holding the
// named one does not exclude, or creates the history table with the default
// engine, which on a replicated ClickHouse cluster exists on one node.
var lockEngineVariables = []string{"PTAH_LOCK_NAME", "PTAH_MIGRATION_LOCK_NAME", "PTAH_MIGRATIONS_ENGINE"}

// minLockEngineVariableBindings is a floor on the bindings the walk finds, so
// a walk that stopped matching cannot pass by finding nothing. Both trees bind
// one of the variables on 19 flags.
const minLockEngineVariableBindings = 19

// lockEngineVariableBindings walks tree and returns every flag bound to one of
// lockEngineVariables.
func lockEngineVariableBindings(tree *cobra.Command) []boundFlag {
	var found []boundFlag
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
			variable, bound := envbinding.Of("PTAH", flag)
			if bound && slices.Contains(lockEngineVariables, variable) {
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

// TestEveryLockAndEngineVariableRefusesAnEmptyValue holds every binding of
// lockEngineVariables on both binaries to the refusal, so a flag added without
// it fails. Each rule is asked to refuse the empty value too, so a declaration
// that refuses nothing does not pass for one in effect.
func TestEveryLockAndEngineVariableRefusesAnEmptyValue(t *testing.T) {
	c := qt.New(t)
	bindings := slices.Concat(
		lockEngineVariableBindings(root.NewRootCommand()),
		lockEngineVariableBindings(cmdrefviews.Compat(atlascompatpolicy.Full())),
	)

	c.Assert(len(bindings) >= minLockEngineVariableBindings, qt.IsTrue, qt.Commentf("found %d bindings", len(bindings)))
	for _, binding := range bindings {
		rule, refused := envbinding.EmptyRefused(binding.flag)
		comment := qt.Commentf("%s --%s reads %s", binding.command, binding.flag.Name, binding.variable)
		c.Check(refused, qt.IsTrue, comment)
		c.Check(rule.Check(binding.variable, binding.flag.Name, ""), qt.ErrorIs, envbinding.ErrEmptyValue, comment)
	}
}

// compatEngineVerbs are the `ptah-compat` verbs that read
// PTAH_MIGRATIONS_ENGINE without a flag, since the community binary registers
// none, with the arguments that reach the run. `migrate down` without --format
// forwards to `ptah migrations down`, whose flag binding reads it, and refuses
// an empty value itself before it forwards, since the binding's refusal names
// a flag the verb does not have.
var compatEngineVerbs = []struct {
	name string
	args []string
}{
	{name: "migrate apply", args: []string{"migrate", "apply", "--url", "sqlite://app.db", "--dir", "file://migrations"}},
	{name: "migrate status", args: []string{"migrate", "status", "--url", "sqlite://app.db", "--dir", "file://migrations"}},
	{name: "migrate set", args: []string{"migrate", "set", "1", "--url", "sqlite://app.db", "--dir", "file://migrations"}},
	{
		name: "migrate down with --format",
		args: []string{"migrate", "down", "--url", "sqlite://app.db", "--dir", "file://migrations", "--format", "{{ . }}"},
	},
	{name: "migrate down", args: []string{"migrate", "down", "--url", "sqlite://app.db", "--dir", "file://migrations"}},
}

// compatEngineTree is the compatibility tree the verbs run on.
func compatEngineTree() *cobra.Command { return cmdrefviews.Compat(atlascompatpolicy.Full()) }

// TestCompatVerbsRefuseAnEmptyMigrationsEngineVariable runs each verb with
// PTAH_MIGRATIONS_ENGINE empty and blank, in a directory holding nothing. The
// run refuses before it opens the database, so app.db is not created. Read as
// unset, the variable would create the revision table with the default
// engine, which the table keeps.
func TestCompatVerbsRefuseAnEmptyMigrationsEngineVariable(t *testing.T) {
	values := []struct {
		value, state string
	}{
		{value: "", state: "set but empty"},
		{value: " ", state: `set to " ", which names nothing`},
	}
	for _, value := range values {
		for _, verb := range compatEngineVerbs {
			t.Run(verb.name+"/"+strconv.Quote(value.value), func(t *testing.T) {
				c := qt.New(t)
				dir := c.TempDir()
				c.Chdir(dir)
				c.Setenv("PTAH_MIGRATIONS_ENGINE", value.value)

				err := projectEnvRun(c, compatEngineTree, verb.args, nil)

				c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`PTAH_MIGRATIONS_ENGINE is `+value.state+
					`: give it a value, or unset it to use the dialect's default engine`))
				_, statErr := os.Stat(filepath.Join(dir, "app.db"))
				c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
			})
		}
	}
}

// TestCompatVerbsReadAnUnsetMigrationsEngineVariable is the control for the
// refusal: with the variable unset, each verb goes on to fail for its own
// reason, the missing directory.
func TestCompatVerbsReadAnUnsetMigrationsEngineVariable(t *testing.T) {
	for _, verb := range compatEngineVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())
			c.Setenv("PTAH_MIGRATIONS_ENGINE", "MergeTree")
			c.Assert(os.Unsetenv("PTAH_MIGRATIONS_ENGINE"), qt.IsNil)

			err := projectEnvRun(c, compatEngineTree, verb.args, nil)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.Not(qt.ErrorIs), envbinding.ErrEmptyValue)
		})
	}
}
