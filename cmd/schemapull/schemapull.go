// Package schemapull implements desired-schema retrieval from OCI registries.
package schemapull

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/schemaartifact"
)

type options struct {
	output    string
	plainHTTP bool
}

// NewSchemaPullCommand returns the schema pull command.
func NewSchemaPullCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "pull <oci-reference>",
		Short: "Pull a desired schema from an OCI registry",
		Long: `Pull and validate a Ptah desired-schema artifact from an OCI-compliant
registry, then write its canonical HCL representation to a new local file.
Authentication comes from the Docker credential store.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.output, "out", "", "Canonical HCL output file to create (required)")
	flags.BoolVar(&opts.plainHTTP, "plain-http", false, "Use plain HTTP for an explicitly trusted local registry")
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	artifact, output, err := schemaartifact.PullToFile(
		cmd.Context(),
		reference,
		opts.output,
		opts.plainHTTP,
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"Pulled %s to %s\nDigest: %s\n",
		artifact.Reference,
		output,
		artifact.Descriptor.Digest,
	)
	return nil
}
