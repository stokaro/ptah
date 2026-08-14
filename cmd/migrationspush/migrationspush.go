// Package migrationspush implements migration artifact publication to OCI
// registries.
package migrationspush

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

type options struct {
	migrationsDir string
	tags          []string
	version       string
	dirFormat     string
	verifySum     bool
	plainHTTP     bool
}

// NewMigrationsPushCommand returns the migrations push command.
func NewMigrationsPushCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "push <oci-reference>",
		Short: "Push a migration directory to an OCI registry",
		Long: `Push a migration directory to an OCI-compliant registry as an immutable
OCI 1.1 artifact. The artifact is tagged with latest, an automatically generated
UTC version tag, and every explicit --tag. Authentication comes from the Docker
credential store; --plain-http is intended only for explicitly trusted local
registries.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.migrationsDir, "migrations-dir", "./migrations", "Migration directory to publish")
	flags.StringArrayVar(&opts.tags, "tag", nil, "Additional movable tag to apply (repeatable)")
	flags.StringVar(&opts.version, "version", "", "Write-once version tag (defaults to v<UTC timestamp>)")
	flags.StringVar(&opts.dirFormat, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.BoolVar(&opts.verifySum, "verify-sum", false, migrationsource.VerifySumUsage(
		"Require the local migration directory to match its ptah.sum or atlas.sum before "+
			"pushing, and fail when it carries neither",
	))
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	result, err := migrationartifact.PushDirectory(cmd.Context(), migrationartifact.DirectoryPushOptions{
		Reference: reference,
		Directory: opts.migrationsDir,
		Tags:      opts.tags,
		Version:   opts.version,
		DirFormat: dirFormat,
		PlainHTTP: opts.plainHTTP,
		VerifySum: opts.verifySum,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"Pushed %s as %s\nDigest: %s\nVersion: %s\nTags: %v\n",
		result.Directory,
		result.Reference,
		result.Descriptor.Digest,
		result.Version,
		result.Tags,
	)
	return nil
}
