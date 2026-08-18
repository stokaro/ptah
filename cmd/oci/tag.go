package oci

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
)

type tagOptions struct {
	plainHTTP bool
}

func newTagCommand() *cobra.Command {
	opts := tagOptions{}
	cmd := &cobra.Command{
		Use:   "tag <oci-reference> <tag> [tag...]",
		Short: "Move an alias onto an artifact that already exists",
		Long: `Move an alias onto an artifact that already exists.

Promotion through a push re-derives content that was already reviewed, and the
artifact that arrives in production is then an equal one rather than the same
one. This moves the alias instead: the manifest digest is unchanged by
construction, because nothing is built and nothing is uploaded.

    ptah oci tag ghcr.io/acme/db@sha256:... staging
    ptah oci tag ghcr.io/acme/db:staging production

Aliases move one at a time. If a later one fails, the ones already applied are
reported by name, because an operator told only that it failed still has to go
and find out which environment now points at the new build.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTag(cmd, args[0], args[1:], opts)
		},
	}
	dbcli.RegisterPlainHTTPFlag(cmd.Flags(), &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cobra.MinimumNArgs(2))
	return cmd
}

func runTag(cmd *cobra.Command, reference string, tags []string, opts tagOptions) error {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	descriptor, applied, err := client.Retag(cmd.Context(), reference, tags)
	if err != nil {
		if len(applied) > 0 {
			fmt.Fprintf(cmd.ErrOrStderr(), "Applied before the failure: %v\n", applied)
		}
		return err
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Digest: %s\n", descriptor.Digest)
	fmt.Fprintf(out, "Tags:   %v\n", applied)
	return nil
}
