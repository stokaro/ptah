package featureinventory

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Tree names one command tree this repository ships.
//
// There are three rather than two because PTAH_ATLAS_STRICT_COMPAT=1 does not
// narrow behavior inside a fixed tree, it registers a different one: 53 command
// paths become 25. An inventory that knew only the default profile would
// describe one of two shipped surfaces.
type Tree string

const (
	// TreeNative is the native `ptah` binary.
	TreeNative Tree = "ptah"
	// TreeCompat is `ptah-compat` under the default compatibility policy.
	TreeCompat Tree = "ptah-compat"
	// TreeCompatStrict is `ptah-compat` under PTAH_ATLAS_STRICT_COMPAT=1.
	TreeCompatStrict Tree = "ptah-compat-strict"
)

// Trees lists every censused tree in the order the generated reference renders
// them.
func Trees() []Tree { return []Tree{TreeNative, TreeCompat, TreeCompatStrict} }

// Launcher is the program name a tree answers to. Two trees share one, which is
// the point: the strict profile is the same executable under an environment
// variable, so a documented invocation cannot be attributed to one of them by
// its spelling.
func (t Tree) Launcher() string {
	if t == TreeNative {
		return "ptah"
	}
	return "ptah-compat"
}

// Flag is one flag registration as the tree carries it, not as help renders it.
type Flag struct {
	Name string
	// Shorthand is the single-letter spelling, empty when there is none.
	Shorthand string
	// Hidden is pflag's own bit. It decides both directions of the flag
	// coverage rule: a hidden flag need not be documented, and a hidden flag
	// named in documentation is still real.
	Hidden bool
	// NoOptDefVal is pflag's own field, and it says whether the next word on
	// the line is this flag's value or a positional argument. `--dry-run x`
	// passes one positional and `--db-url x` passes none, and nothing but this
	// field separates them.
	NoOptDefVal string
	Usage       string
}

// Command is one path in one tree.
type Command struct {
	Tree Tree
	// Path is the command path without the launcher: "schema render". The root
	// carries the empty string.
	Path string
	// Hidden is cobra's own bit. Strict mode keeps twelve gated commands
	// registered and hidden so that invoking one produces a named abort rather
	// than `unknown command`, and a census that dropped them would report the
	// abort as unreachable.
	Hidden bool
	// Leaf reports that the command registers no subcommands.
	Leaf bool
	// Validate is the command's own positional-argument validator, bound to the
	// command and ready to be asked about a concrete argument list. Walk sets
	// it for every command it produces, so a Census always carries one.
	//
	// It is asked rather than described, and never inferred from Leaf: a
	// namespace that loses its last subcommand becomes a leaf and still refuses
	// every word its verbs used to answer to. Asking it with the words a
	// document actually wrote is also the only reading that gets an arity or a
	// type right -- `ptah oci tag <ref> <tag>` needs two, and
	// `ptah-compat migrate apply 2` needs an integer.
	Validate func(args []string) error
	Short    string
	// Flags holds every flag the command parses, including the persistent flags
	// it inherits, keyed by name.
	Flags map[string]Flag
}

// Qualified is the path with its launcher, which is how documentation spells it.
func (c Command) Qualified() string {
	return strings.TrimSpace(c.Tree.Launcher() + " " + c.Path)
}

// Census is every command of every tree.
type Census struct {
	Commands []Command
	byKey    map[string]Command
	byTree   map[Tree][]Command
}

// NewCensus assembles a census from one already-constructed root per tree.
//
// The roots are passed in rather than constructed here because the compat roots
// live in cmd/atlas, and AGENTS.md holds cmd/ptah-compat/main.go to being the
// only non-test file outside that tree which imports it. Taking the roots as an
// argument keeps this package free of the command layer entirely, and lets a
// test drive it with a tree of two commands.
func NewCensus(roots map[Tree]*cobra.Command) (*Census, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("featureinventory: no command trees to census; refusing to report a surface measured from nothing")
	}
	if err := AssertCobraInitializerParity(); err != nil {
		return nil, err
	}
	census := &Census{byKey: make(map[string]Command), byTree: make(map[Tree][]Command)}
	for _, tree := range Trees() {
		root, ok := roots[tree]
		if !ok {
			continue
		}
		walked := Walk(tree, root)
		if len(walked) == 0 {
			return nil, fmt.Errorf("featureinventory: tree %q walked to no commands at all", tree)
		}
		census.Commands = append(census.Commands, walked...)
		census.byTree[tree] = walked
	}
	for _, cmd := range census.Commands {
		census.byKey[string(cmd.Tree)+"\x00"+cmd.Path] = cmd
	}
	return census, nil
}

// Lookup returns one command by tree and path.
func (c *Census) Lookup(tree Tree, path string) (Command, bool) {
	cmd, ok := c.byKey[string(tree)+"\x00"+path]
	return cmd, ok
}

// OfTree returns one tree's commands, in walk order.
func (c *Census) OfTree(tree Tree) []Command { return c.byTree[tree] }

// Paths returns one tree's command paths, sorted, with the root's empty path
// included.
func (c *Census) Paths(tree Tree) []string {
	paths := make([]string, 0, len(c.byTree[tree]))
	for _, cmd := range c.byTree[tree] {
		paths = append(paths, cmd.Path)
	}
	sort.Strings(paths)
	return paths
}

// VisiblePaths is Paths without the commands cobra marks hidden.
func (c *Census) VisiblePaths(tree Tree) []string {
	paths := make([]string, 0, len(c.byTree[tree]))
	for _, cmd := range c.byTree[tree] {
		if cmd.Hidden {
			continue
		}
		paths = append(paths, cmd.Path)
	}
	sort.Strings(paths)
	return paths
}

// Resolve walks a launcher's positional words down the tree and reports how far
// they got.
//
// It returns the deepest command the words reach, the index of the first word
// that command could not consume, and whether that command would refuse the
// positional tail it was left holding. That last answer is what separates a
// stale command reference from an ordinary positional argument, and it is
// obtained by asking the command's own validator rather than by describing it.
//
// Flags are stepped over the way cobra steps over them, which needs the flag
// set: `--dry-run x` leaves one positional word behind and `--db-url x` leaves
// none. Reading every non-flag word as positional would hand a validator a URL
// and call the document stale.
func (c *Census) Resolve(tree Tree, words []string) (cmd Command, consumed int, refused bool) {
	current, ok := c.Lookup(tree, "")
	if !ok {
		return Command{}, 0, false
	}
	rest := words
	offset := 0
	for {
		positions := positionalIndexes(rest, current)
		if len(positions) == 0 {
			return current, len(words), false
		}
		index := positions[0]
		child, found := c.Lookup(tree, strings.TrimSpace(current.Path+" "+rest[index]))
		if !found {
			tail := make([]string, 0, len(positions))
			for _, at := range positions {
				tail = append(tail, rest[at])
			}
			return current, offset + index, current.Validate(tail) != nil
		}
		current = child
		offset += index + 1
		rest = rest[index+1:]
	}
}

// positionalIndexes reports which of a command line's words a command would see
// as positional arguments.
//
// It mirrors cobra's own stripFlags, including the two cases that decide whether
// the next word is a flag's value: a long flag with no `=` whose flag takes a
// value, and a two-character short flag whose flag takes one. A flag this
// command does not register is treated as taking a value, which is the
// conservative direction: the alternative reports the value as a stray
// positional and blames the document for a flag name the check has already
// reported once.
func positionalIndexes(words []string, cmd Command) []int {
	var positions []int
	for index := 0; index < len(words); index++ {
		word := words[index]
		switch {
		case word == "--":
			return positions
		case strings.HasPrefix(word, "--"):
			if !strings.Contains(word, "=") && cmd.takesValue(strings.TrimPrefix(word, "--")) {
				index++
			}
		case strings.HasPrefix(word, "-") && word != "-":
			if len(word) == 2 && !strings.Contains(word, "=") && cmd.shorthandTakesValue(word[1:]) {
				index++
			}
		case IsPlaceholder(word):
			// Kept in the line so the flag before it still pairs with its
			// value, and skipped here so no validator is asked about `[flags]`.
		default:
			positions = append(positions, index)
		}
	}
	return positions
}

// takesValue reports whether a long flag consumes the following word.
func (c Command) takesValue(name string) bool {
	flag, registered := c.Flags[name]
	return !registered || flag.NoOptDefVal == ""
}

// shorthandTakesValue is takesValue for a single-letter spelling.
func (c Command) shorthandTakesValue(shorthand string) bool {
	for _, flag := range c.Flags {
		if flag.Shorthand == shorthand {
			return flag.NoOptDefVal == ""
		}
	}
	return true
}

// Walk returns every command of one tree, including the root.
//
// It calls the two initializers cobra's ExecuteC calls before dispatch, so the
// tree measured here is the tree the binary answers with rather than the tree
// the constructor returned. The difference is not cosmetic: on the native root
// it is six paths -- `completion`, its four shells, and `help` -- and a walk
// that skipped them reported 97 where the binary ships 103. That walk is what
// produced docs/agent-surface.md, whose generated block holds 86 rows against
// 91 leaves and contains the word `completion` zero times.
//
// The same two initializers are called per command for the flag set, because
// cobra adds `--help` in execute() rather than at construction. A plain flag
// walk of a constructed tree misses `--help` on every command in it.
func Walk(tree Tree, root *cobra.Command) []Command {
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()

	var out []Command
	var visit func(cmd *cobra.Command, parentPath string)
	visit = func(cmd *cobra.Command, parentPath string) {
		path := strings.TrimSpace(parentPath + " " + cmd.Name())
		if cmd.Parent() == nil {
			path = ""
		}
		out = append(out, Command{
			Tree:     tree,
			Path:     path,
			Hidden:   cmd.Hidden,
			Leaf:     !cmd.HasSubCommands(),
			Validate: validatorOf(cmd),
			Short:    cmd.Short,
			Flags:    flagsOf(cmd),
		})
		for _, sub := range cmd.Commands() {
			visit(sub, path)
		}
	}
	visit(root, "")
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// validatorOf returns one command's positional-argument validator, bound.
//
// cobra applies legacyArgs when Args is nil, which refuses a leftover word only
// at the ROOT of a tree that has subcommands and accepts it everywhere else, so
// a nil validator is reproduced here rather than treated as "no opinion".
//
// The returned closure writes nothing: cmdutil.Fail prints its diagnostic to the
// command's own error writer, so the writers are redirected first. Walk already
// mutates the tree it is given -- it runs cobra's two default initializers -- so
// the census takes ownership of the roots it is handed, and this is not a new
// claim on them.
func validatorOf(cmd *cobra.Command) func(args []string) error {
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if cmd.Args == nil {
		hasParent, hasSubCommands := cmd.HasParent(), cmd.HasSubCommands()
		return func(args []string) error {
			if hasParent || !hasSubCommands || len(args) == 0 {
				return nil
			}
			return fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())
		}
	}
	return func(args []string) error { return cmd.Args(cmd, args) }
}

// flagsOf reads one command's complete flag set.
func flagsOf(cmd *cobra.Command) map[string]Flag {
	cmd.InitDefaultHelpFlag()
	cmd.InitDefaultVersionFlag()
	// InheritedFlags has the side effect of merging every ancestor's persistent
	// flags into Flags(), which is what execution does before parsing. Without
	// it a documented `--config` on a leaf reads as a flag that does not exist.
	cmd.InheritedFlags()

	flags := make(map[string]Flag)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		flags[f.Name] = Flag{Name: f.Name, Shorthand: f.Shorthand, Hidden: f.Hidden, NoOptDefVal: f.NoOptDefVal, Usage: f.Usage}
	})
	return flags
}

// AssertCobraInitializerParity measures whether the two exported initializers
// [cobra.Command.InitDefaultHelpCmd] and
// [cobra.Command.InitDefaultCompletionCmd] still reproduce the tree ExecuteC
// dispatches against.
//
// This is the alternative to exempting `completion` and `help` by name. An
// exemption written as two strings is a claim about cobra that nothing rechecks;
// this runs cobra twice on a throwaway command and compares the answers, so a
// third initializer added upstream fails here instead of quietly shortening
// every census below it.
//
// The measurement also settles what happens to `__complete`. ExecuteC calls the
// unexported initCompleteCmd, which registers it and then removes it again
// unless the invocation being served IS a completion request -- so `__complete`
// is not a path the shipped tree carries, and a census that omits it omits
// nothing. The probe asserts both halves: parity for an ordinary invocation, and
// `__complete` present only while a completion request is served.
func AssertCobraInitializerParity() error {
	newProbe := func() *cobra.Command {
		root := &cobra.Command{Use: "probe", Run: func(*cobra.Command, []string) {}}
		root.AddCommand(&cobra.Command{Use: "child", Run: func(*cobra.Command, []string) {}})
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		// An EMPTY slice, never nil. ExecuteC falls back to os.Args when a
		// command's args are nil, so a nil here would hand the probe whatever
		// arguments the test binary was run with.
		root.SetArgs(make([]string, 0))
		return root
	}

	exported := newProbe()
	exported.InitDefaultHelpCmd()
	exported.InitDefaultCompletionCmd()
	viaInitializers := probeNames(exported)

	dispatched := newProbe()
	if _, err := dispatched.ExecuteC(); err != nil {
		return fmt.Errorf("featureinventory: the cobra parity probe could not be executed: %w", err)
	}
	viaExecuteC := probeNames(dispatched)

	if !slices.Equal(viaInitializers, viaExecuteC) {
		return fmt.Errorf(
			"featureinventory: cobra's ExecuteC no longer registers what InitDefaultHelpCmd and InitDefaultCompletionCmd register\n"+
				"  via the exported initializers: %v\n"+
				"  via ExecuteC:                  %v\n"+
				"  a census built on the exported initializers would now be short by the difference",
			viaInitializers, viaExecuteC)
	}

	completing := newProbe()
	completing.SetArgs([]string{cobra.ShellCompRequestCmd, ""})
	if _, err := completing.ExecuteC(); err != nil {
		return fmt.Errorf("featureinventory: the cobra completion probe could not be executed: %w", err)
	}
	served := probeNames(completing)
	if slices.Equal(served, viaExecuteC) {
		return fmt.Errorf(
			"featureinventory: cobra no longer registers %s while serving a completion request; the census's account of it is stale",
			cobra.ShellCompRequestCmd)
	}
	for _, name := range served {
		if slices.Contains(viaExecuteC, name) {
			continue
		}
		if name == cobra.ShellCompRequestCmd || name == "alias:"+cobra.ShellCompNoDescRequestCmd {
			continue
		}
		return fmt.Errorf(
			"featureinventory: serving a completion request now registers %q, which is neither %s nor its alias; the census may be missing a shipped path",
			name, cobra.ShellCompRequestCmd)
	}
	return nil
}

// probeNames lists every command name and alias below a root, sorted.
func probeNames(root *cobra.Command) []string {
	var names []string
	var visit func(cmd *cobra.Command)
	visit = func(cmd *cobra.Command) {
		for _, sub := range cmd.Commands() {
			names = append(names, sub.Name())
			for _, alias := range sub.Aliases {
				names = append(names, "alias:"+alias)
			}
			visit(sub)
		}
	}
	visit(root)
	sort.Strings(names)
	return names
}
