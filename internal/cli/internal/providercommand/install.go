package providercommand

import (
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
)

// connectionFlags are the flags that make a verb name a database or a dialect:
// a database URL, a dev-database URL, a dialect, or a project environment,
// whose blocks name the URLs. A verb that declares one reads an engine, so it
// selects a provider for that engine.
var connectionFlags = []string{"db-url", "dev-url", "dialect", "env"}

// selectsNoProvider names the native verbs that declare a connection flag and
// still select no provider, each with the reason. A verb is added here only
// with a reason that holds for every engine.
var selectsNoProvider = map[string]string{
	"ptah assist":          assistSelectsNoProvider,
	"ptah assist context":  assistSelectsNoProvider,
	"ptah assist explain":  assistSelectsNoProvider,
	"ptah project inspect": "reads the project file and opens no database",
}

// assistSelectsNoProvider is why `ptah assist` selects no provider: the agent
// tools run in process for a person at a terminal, `ptah mcp` is the surface
// that serves them through the operator's provider, and agentflags keeps the
// provider flags off the shared tool flags on purpose.
const assistSelectsNoProvider = "serves the agent tools in process; ptah mcp selects the operator's provider"

// Install gives every leaf of root that names a database or a dialect the
// provider selection [Configure] installs, with [cmdutil.Fail] as its failure
// report. A leaf that already selects a provider keeps its own report, which is
// how a namespace with a machine-readable failure shape keeps it. It is called
// once, after the tree is assembled.
//
// A verb is in scope when it declares one of the connection flags, so a verb
// added later with --db-url or --dialect selects a provider without being named
// here; [SelectsNoProvider] lists the exceptions.
func Install(root *cobra.Command) {
	for _, leaf := range leaves(root) {
		if InScope(leaf) && leaf.Flags().Lookup(ConfigFlag) == nil {
			Configure(leaf, cmdutil.Fail)
		}
	}
}

// InScope reports whether a verb names a database or a dialect and is not one of
// the exceptions [SelectsNoProvider] lists.
func InScope(cmd *cobra.Command) bool {
	if _, excepted := selectsNoProvider[cmd.CommandPath()]; excepted {
		return false
	}
	return slices.ContainsFunc(connectionFlags, func(name string) bool { return cmd.Flags().Lookup(name) != nil })
}

// SelectsNoProvider returns the command paths of the verbs that name a database
// or a dialect and select no provider, with the reason for each.
func SelectsNoProvider() map[string]string {
	return maps.Clone(selectsNoProvider)
}

// leaves returns every command below root that runs through RunE, which is the
// function [Configure] wraps. A group runs too, to print its help, and a group
// such as `ptah assist` does its own work; whether a command names an engine
// is [InScope]'s question, not the tree's shape.
func leaves(root *cobra.Command) []*cobra.Command {
	var result []*cobra.Command
	for _, child := range root.Commands() {
		result = append(result, leaves(child)...)
		if child.RunE != nil {
			result = append(result, child)
		}
	}
	return result
}
