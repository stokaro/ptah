package agentsurface

import (
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/internal/envbinding"
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

// Flag is one flag a command registers, as pflag holds it.
type Flag struct {
	// Name and Shorthand are the two spellings the command line accepts, the
	// shorthand empty when the flag has none.
	Name      string
	Shorthand string
	// Type and Default are pflag's own answers: the value type it will parse
	// and the value it holds when nothing sets one.
	Type    string
	Default string
	// Usage is the flag's own words, including the `[env: PTAH_X]` suffix the
	// binding installer appends to them.
	Usage string
	// Environment is the variable this flag reads when it is not typed on the
	// command line, empty when it reads none. It is asked of
	// [envbinding.Of] rather than read back out of Usage, because the suffix
	// there is a rendering of this answer and re-reading a rendering is how
	// the two spellings come apart.
	Environment string
	// Hidden reports that `--help` does not list the flag. The flag still
	// parses, which is why the reference prints it and marks it.
	Hidden bool
	// Persistent reports that the flag was declared on this command's
	// persistent set, so every command below it accepts it too. Six of the
	// eight are declared this way on `ptah-compat`: `--config`, `--env` and
	// `--var` on the `migrate` and `schema` groups. A record that visited only
	// the local set would find those groups carrying no flags at all, and
	// every verb below them missing three the binary accepts.
	Persistent bool
}

// Node is one command of the tree as the binary ships it, group or leaf.
type Node struct {
	// Name is the command line spelling without the program name, so a group
	// and its children differ by one word: "schema", "schema inspect".
	Name string
	// Summary is cmd.Short, the line the parent's help listing prints.
	Summary string
	// Hidden reports that the command is registered and absent from `--help`.
	// It is how the Atlas-compatible tree refuses a verb under
	// PTAH_ATLAS_STRICT_COMPAT without unregistering it.
	Hidden bool
	// Leaf reports that the command has no subcommands, and Runnable that
	// cobra will call it rather than print help. They differ on a group with a
	// RunE, which is what decides whether an unregistered child's invocation
	// reaches an error or the group's help.
	Leaf     bool
	Runnable bool
	// Flags are the flags this command registers itself, in pflag's order.
	Flags []Flag
}

// Nodes returns every command of a tree, group and leaf alike, sorted by name.
//
// The tree a binary is CONSTRUCTED from is not the tree it SHIPS, so the walk
// finishes the construction the way cobra does before it measures anything.
// [cobra.Command.ExecuteC] calls InitDefaultHelpCmd and InitDefaultCompletionCmd
// after the program has assembled its own tree, and those supply `help` and
// `completion` with one leaf per shell. Measured: `root.NewRootCommand()` walks
// to 86 leaves, and the shipped `ptah` answers to the 91 its `--help` lists.
// Without these two calls the inventory describes the constructor, and the five
// verbs an agent can actually reach are missing from a document whose first
// claim is that it answers for every verb.
//
// Both initializers are idempotent, which is what makes calling them here a
// completion of the tree rather than an override of it. `ptah-compat` is the
// case that proves it: cmd/atlas registers `completion` itself, calling
// InitDefaultCompletionCmd at cmd/atlas/atlas.go:469 so that Atlas-style group
// help can rewrite the command afterwards. That tree is short by `help` alone,
// not by six names, and InitDefaultCompletionCmd returns early against it
// rather than replacing the rewritten command.
//
// Two spellings the shipped binary answers to stay outside this walk, and no
// walk can reach them: `__complete` and its alias `__completeNoDesc` come from
// cobra's unexported initCompleteCmd, which also removes the command again
// unless the invocation being parsed is that command. They are hidden, they
// answer a shell rather than a person or an agent, and a row for either would
// have to be hand-written and therefore unchecked -- which is the failure this
// package exists to prevent. docs/agent-surface.md names them in prose instead,
// outside the generated tables, where a claim nothing measures belongs.
//
// [Walk] is the second view of this one traversal and NOT a second traversal,
// which is AGENTS.md's rule "Recognition that spans two functions belongs to
// one of them" read at the place it applies here. The two paragraphs above are
// the recognition: which commands a shipped tree holds is decided by two
// initializer calls whose correctness differs per binary, and a walk that
// restated them would be wrong about `ptah-compat` on the day `cmd/atlas`
// changed which of them it makes for itself. So Nodes is the only place in
// this repository that visits a `*cobra.Command`'s children.
func Nodes(root *cobra.Command) []Node {
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()

	var nodes []Node
	var visit func(cmd *cobra.Command, path string)
	visit = func(cmd *cobra.Command, path string) {
		// The program name is not part of a verb's identity: `ptah` is what the
		// binary happens to be called, and the classification keys are the
		// words after it.
		name := strings.TrimSpace(path + " " + cmd.Name())
		if cmd == root {
			name = ""
		}
		if name != "" {
			nodes = append(nodes, newNode(cmd, name))
		}
		for _, sub := range cmd.Commands() {
			visit(sub, name)
		}
	}
	visit(root, "")
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes
}

// newNode records one command and the flags it registers.
//
// The environment prefix is asked of the COMMAND rather than of the root,
// because the two answers differ and the root's is wrong. `ptah` installs its
// binding while assembling its own tree; `help` and the four `completion`
// leaves are supplied afterwards by cobra and never reach the installer, so
// they carry no wrapper to read a variable with. Measured: taking the root's
// prefix credits `completion bash --no-descriptions` with a
// `PTAH_NO_DESCRIPTIONS` binding that nothing anywhere reads.
func newNode(cmd *cobra.Command, name string) Node {
	prefix, bound := envbinding.InstalledPrefix(cmd)
	node := Node{
		Name:     name,
		Summary:  cmd.Short,
		Hidden:   cmd.Hidden,
		Leaf:     !cmd.HasSubCommands(),
		Runnable: cmd.Runnable(),
	}
	// Persistent first: a flag declared on both sets is one flag, and the
	// persistent declaration is the one that describes where it applies.
	seen := make(map[string]bool)
	record := func(persistent bool) func(*pflag.Flag) {
		return func(flag *pflag.Flag) {
			if seen[flag.Name] {
				return
			}
			seen[flag.Name] = true
			recorded := Flag{
				Name:       flag.Name,
				Shorthand:  flag.Shorthand,
				Type:       flag.Value.Type(),
				Default:    flag.DefValue,
				Usage:      flag.Usage,
				Hidden:     flag.Hidden,
				Persistent: persistent,
			}
			if bound {
				if variable, reads := envbinding.Of(prefix, flag); reads {
					recorded.Environment = variable
				}
			}
			node.Flags = append(node.Flags, recorded)
		}
	}
	cmd.PersistentFlags().VisitAll(record(true))
	cmd.Flags().VisitAll(record(false))
	sort.Slice(node.Flags, func(i, j int) bool { return node.Flags[i].Name < node.Flags[j].Name })
	return node
}

// Walk returns every runnable leaf of a command tree, sorted by name.
//
// A group with subcommands is not a leaf even when cobra will run it: running
// `ptah schema` prints help, and a help printer is not an operation anyone
// classifies.
//
// It is a view of [Nodes] rather than a walk of its own; see there for why the
// traversal cannot be duplicated.
func Walk(root *cobra.Command) []Leaf {
	var leaves []Leaf
	for _, node := range Nodes(root) {
		if node.Leaf {
			leaves = append(leaves, newLeaf(node))
		}
	}
	return leaves
}

// newLeaf records one command's database-naming flags.
func newLeaf(node Node) Leaf {
	leaf := Leaf{Name: node.Name, Summary: node.Summary, ScratchUsage: make(map[string]string)}
	for _, flag := range node.Flags {
		if slices.Contains(targetFlags, flag.Name) {
			leaf.TargetFlags = append(leaf.TargetFlags, flag.Name)
		}
		if slices.Contains(scratchFlags, flag.Name) {
			leaf.ScratchFlags = append(leaf.ScratchFlags, flag.Name)
			leaf.ScratchUsage[flag.Name] = flag.Usage
		}
	}
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
