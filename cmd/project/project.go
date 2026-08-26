// Package project implements `ptah project`: the verbs that read a project
// file and say what Ptah makes of it.
//
// It exists for the adoption path in stokaro/ptah#1215. A team arriving from
// Atlas replaces the binary with ptah-compat and keeps its atlas.hcl, and
// nothing then tells them which parts of that file Ptah acts on, which are
// carried by a compatibility adapter, and which are read and ignored. This is
// the verb that answers, before any of it has to be rewritten.
package project

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

// NewProjectCommand returns the `project` command group.
func NewProjectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "project",
		Short: "Read a project file and report what Ptah makes of it",
		Long: `Read a project file and report what Ptah makes of it.

Ptah accepts two project files: its own ptah.yaml, and Atlas's atlas.hcl for a
project arriving from Atlas. Both are read, and atlas.hcl takes precedence where
they overlap.

What this group adds is the answer to "what does my file actually do here":
which settings Ptah acts on, and which names it accepted and did nothing with.
The second list is the one that cannot be got any other way -- Atlas CE reports
nothing for a name it does not know, so a setting that silently does nothing
looks exactly like one that works.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newProjectInspectCommand())
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
