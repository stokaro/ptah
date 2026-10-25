package root_test

import (
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/cli/root"
)

// connectionFlags are the flags that make a verb name a database or a dialect,
// spelled here rather than read from the package that installs the selection,
// so a flag it stopped reading reddens this census instead of shrinking it.
var connectionFlags = []string{"db-url", "dev-url", "dialect", "env"}

// nativeLeaves returns every command of the native tree that runs, keyed by
// its command path, whether it runs through RunE or Run, and whether or not it
// has subcommands of its own.
func nativeLeaves(cmd *cobra.Command) map[string]*cobra.Command {
	leaves := make(map[string]*cobra.Command)
	for _, child := range cmd.Commands() {
		maps.Copy(leaves, nativeLeaves(child))
		leaves[child.CommandPath()] = child
	}
	return leaves
}

func namesAnEngine(cmd *cobra.Command) bool {
	return slices.ContainsFunc(connectionFlags, func(name string) bool { return cmd.Flags().Lookup(name) != nil })
}

// Every native verb that names a database or a dialect selects a provider for
// the engine it names: it carries the provider flags, which the selection
// registers. A verb that runs through Run rather than RunE is not wrapped by
// the selection, and fails here rather than reaching an engine a provider
// serves through the built-in engines alone.
func TestEveryNativeVerbThatNamesAnEngineSelectsAProvider(t *testing.T) {
	excepted := providercommand.SelectsNoProvider()
	leaves := nativeLeaves(root.NewRootCommand())
	checked := 0
	for path, leaf := range leaves {
		_, isException := excepted[path]
		inScope := namesAnEngine(leaf) && !isException
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(leaf.Flags().Lookup(providercommand.ConfigFlag) != nil || !inScope, qt.IsTrue,
				qt.Commentf("%s names a database or a dialect and selects no provider", path))
		})
		checked += boolToInt(inScope)
	}
	// The census reads the tree it is about: a walk that stopped finding the
	// verbs would report nothing and pass.
	c := qt.New(t)
	c.Assert(checked >= 40, qt.IsTrue, qt.Commentf("only %d verbs name an engine", checked))
}

// Every exception names a verb that exists and names an engine, so an
// exception cannot outlive the verb it was written for and keep a later verb
// of the same path out of scope.
func TestEveryExceptionNamesAVerbThatNamesAnEngine(t *testing.T) {
	leaves := nativeLeaves(root.NewRootCommand())
	for path, reason := range providercommand.SelectsNoProvider() {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			leaf, found := leaves[path]
			c.Assert(found, qt.IsTrue)
			c.Assert(namesAnEngine(leaf), qt.IsTrue)
			c.Assert(leaf.Flags().Lookup(providercommand.ConfigFlag), qt.IsNil)
			c.Assert(reason, qt.Not(qt.Equals), "")
		})
	}
}

func boolToInt(value bool) int {
	return map[bool]int{true: 1}[value]
}
