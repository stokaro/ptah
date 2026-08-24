package agentsurface

import (
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// targetFlags and scratchFlags are the flags that name a database.
//
// They are listed rather than matched by shape, because the shape does not
// separate them: `--db-url` names the target for most verbs and a throwaway
// database for the two test runners, and `--shadow-db` names a scratch database
// without carrying "url" in its name at all. A rule that guessed from the
// spelling is precisely the rule ADR 0002 §1.2 used, and it put `schema test`
// in the wrong row.
var (
	targetFlags  = []string{"db-url"}
	scratchFlags = []string{"dev-url", "shadow-db"}
)

// Leaf is one runnable command as the tree carries it.
type Leaf struct {
	// Name is the command line spelling without the program name, so it is the
	// key [Lookup] takes: "schema inspect".
	Name string
	// Summary is the command's own one-line description.
	Summary string
	// TargetFlags and ScratchFlags are the database-naming flags this command
	// registers, in the order the lists above give them.
	TargetFlags  []string
	ScratchFlags []string
	// ScratchUsage carries each scratch flag's own words, which is where the
	// destructive part is usually stated outright.
	ScratchUsage map[string]string
}

// Walk returns every runnable leaf of a command tree, sorted by name.
//
// A group with subcommands is not a leaf even when cobra will run it: running
// `ptah schema` prints help, and a help printer is not an operation anyone
// classifies.
func Walk(root *cobra.Command) []Leaf {
	var leaves []Leaf
	var visit func(cmd *cobra.Command, path string)
	visit = func(cmd *cobra.Command, path string) {
		// The program name is not part of a verb's identity: `ptah` is what the
		// binary happens to be called, and the classification keys are the
		// words after it.
		name := strings.TrimSpace(path + " " + cmd.Name())
		if cmd == root {
			name = ""
		}
		if !cmd.HasSubCommands() && name != "" {
			leaves = append(leaves, newLeaf(cmd, name))
		}
		for _, sub := range cmd.Commands() {
			visit(sub, name)
		}
	}
	visit(root, "")
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].Name < leaves[j].Name })
	return leaves
}

// newLeaf records one command's database-naming flags.
func newLeaf(cmd *cobra.Command, name string) Leaf {
	leaf := Leaf{Name: name, Summary: cmd.Short, ScratchUsage: make(map[string]string)}
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if slices.Contains(targetFlags, flag.Name) {
			leaf.TargetFlags = append(leaf.TargetFlags, flag.Name)
		}
		if slices.Contains(scratchFlags, flag.Name) {
			leaf.ScratchFlags = append(leaf.ScratchFlags, flag.Name)
			leaf.ScratchUsage[flag.Name] = flag.Usage
		}
	})
	sort.Strings(leaf.TargetFlags)
	sort.Strings(leaf.ScratchFlags)
	return leaf
}

// NamesOf lists the leaf names of a walk, sorted.
func NamesOf(leaves []Leaf) []string {
	names := make([]string, 0, len(leaves))
	for _, leaf := range leaves {
		names = append(names, leaf.Name)
	}
	sort.Strings(names)
	return names
}
