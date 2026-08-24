// Package assist implements "ptah assist", the native Ptah surface that talks
// to a model the operator chose, through a key the operator holds.
//
// This is the provider half. The conversational surface is a later phase; what
// is here is what has to work before one is worth having: naming a provider,
// resolving its credential without storing it, and finding out whether the
// model can do what the workflow needs.
package assist

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

// NewCommand returns the assist namespace.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "assist",
		Short: "Work with Ptah through a model you supply",
		Long: `Ptah Assist talks to a model you choose, through a key you hold.

There is no Ptah account, no Ptah-hosted model, and no Ptah AI token. The model
can be a hosted API, a gateway your organization runs, or one running on this
machine -- in the last case nothing about your schema leaves it.

  ptah assist explain <question>   ask about this project, with Ptah's tools answering
  ptah assist provider list        the profiles this machine can reach
  ptah assist provider test        whether one of them works, measured

Every tool the model reaches is the one an external AI client reaches over the
Model Context Protocol -- the same capability broker, the same verification
gates, the same audit record. Ptah Assist gets nothing an external client does
not.

A profile names an endpoint, a model, and a credential REFERENCE such as
env:OPENAI_API_KEY. Ptah never stores a key: the reference is resolved when a
request is made, and a key written directly into configuration is refused.

Profiles are yours, not your projects'. They are read from your own
configuration file and from the environment, never from a file inside the
repository being worked on -- a repository that could define a profile could
point Ptah at an endpoint of its author's choosing with your key attached.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newExplainCommand())
	cmd.AddCommand(newProviderCommand())
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
