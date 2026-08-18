package oci

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
)

const (
	recursiveFlag = "recursive"
	tagFlag       = "tag"
)

type copyOptions struct {
	recursive bool
	tags      []string
	plainHTTP bool
}

func newCopyCommand() *cobra.Command {
	opts := copyOptions{}
	cmd := &cobra.Command{
		Use:   "copy <source-reference> <destination-reference>",
		Short: "Copy an artifact between repositories without rebuilding it",
		Long: `Copy an artifact between repositories without rebuilding it.

The manifest digest is preserved, so the artifact that arrives is the one that
was reviewed rather than one equal to it. The destination reference names the
alias to create; extra aliases can be added with --` + tagFlag + `.

--` + recursiveFlag + ` carries the artifact's referrers with it. Without it the copy
arrives with its lint results, plans, deployment reports and signatures left
behind in the source repository, which is how a promotion silently loses the
evidence it was promoted on.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCopy(cmd, args[0], args[1], opts)
		},
	}
	flags := cmd.Flags()
	flags.BoolVar(&opts.recursive, recursiveFlag, false, "Carry the artifact's referrers with it")
	flags.StringArrayVar(&opts.tags, tagFlag, nil, "Additional alias to apply at the destination (repeatable)")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(2))
	return cmd
}

func runCopy(cmd *cobra.Command, source, destination string, opts copyOptions) error {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	descriptor, err := client.CopyArtifact(cmd.Context(), source, destination, ociartifact.ArtifactCopyOptions{
		Recursive: opts.recursive,
		Tags:      opts.tags,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Digest: %s\n", descriptor.Digest)
	return nil
}
