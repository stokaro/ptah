// Package migrationspull implements migration artifact retrieval from OCI
// registries.
package migrationspull

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/migrationartifact"
)

type options struct {
	output    string
	plainHTTP bool
}

// NewMigrationsPullCommand returns the migrations pull command.
func NewMigrationsPullCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "pull <oci-reference>",
		Short: "Pull a migration directory from an OCI registry",
		Long: `Pull and validate a Ptah migration artifact from an OCI-compliant registry,
then reconstruct the migration directory byte-for-byte. Authentication comes
from the Docker credential store; --plain-http is intended only for explicitly
trusted local registries.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.output, "out", "", "Output directory to create (required)")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	result, err := migrationartifact.PullDirectory(cmd.Context(), migrationartifact.DirectoryPullOptions{
		Reference: reference,
		Output:    opts.output,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"Pulled %s to %s\nDigest: %s\nFormat: %s\n",
		result.Artifact.Reference,
		result.Output,
		result.Artifact.Descriptor.Digest,
		result.Artifact.DirFormat,
	)
	return nil
}
