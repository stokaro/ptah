package atlas

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/envbool"
)

// atlasGatedCommand is a command registered outside the strict profile that
// answers as the tree without it does until its variable is enabled, such as
// `security` and `migrate drift`.
type atlasGatedCommand struct {
	command  *cobra.Command
	variable envbool.Var
}

// gateAtlasRequests decides for the two commands that reach a gated command
// without running a function of it: the help command and shell completion.
// Both are children of root, so root's persistent pre-run runs before them;
// the gate wraps it, and installs last so nothing wraps it in turn.
//
// `help <command>` with a malformed value is refused as every other spelling
// of the command refuses it, at exit 1, because a help function cannot fail
// the run. Completion while a variable is not enabled, malformed included,
// runs against the tree without its command: the command is taken out of its
// parent for the request and put back after it, so cobra offers neither the
// command nor its flags.
func gateAtlasRequests(root *cobra.Command, gated ...atlasGatedCommand) {
	if len(gated) == 0 {
		return
	}
	// The parents are read once, here: a run whose post-run never ran, such
	// as one whose command failed, leaves a command out of its parent until
	// the next run puts it back.
	parents := make([]*cobra.Command, len(gated))
	for i, gate := range gated {
		parents[i] = gate.command.Parent()
	}
	preRun := root.PersistentPreRunE
	postRun := root.PersistentPostRunE
	removed := make([]bool, len(gated))
	root.PersistentPreRunE = func(run *cobra.Command, args []string) error {
		if preRun != nil {
			if err := preRun(run, args); err != nil {
				return err
			}
		}
		for i, gate := range gated {
			switch {
			case run.Name() == cobra.ShellCompRequestCmd && run.Parent() == root:
				if enabled, err := gate.variable.Resolve(); err != nil || !enabled {
					parents[i].RemoveCommand(gate.command)
					removed[i] = true
				}
			case run.Name() == "help" && run.Parent() == root && atlasCommandWithin(root, args, gate.command):
				if _, err := gate.variable.Resolve(); err != nil {
					return failAtlasCommand(run, err)
				}
			}
		}
		return nil
	}
	root.PersistentPostRunE = func(run *cobra.Command, args []string) error {
		for i, gate := range gated {
			if removed[i] {
				parents[i].AddCommand(gate.command)
				removed[i] = false
			}
		}
		if postRun != nil {
			return postRun(run, args)
		}
		return nil
	}
}
