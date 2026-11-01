// Package migrationspush implements migration artifact publication to OCI
// registries.
package migrationspush

import (
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/cli/internal/pushreceipt"
	"ptah.run/internal/migrationartifact"
	"ptah.run/migration/migrationfile"
)

type options struct {
	migrationsDir    string
	tags             []string
	version          string
	dirFormat        string
	verifySum        bool
	plainHTTP        bool
	format           string
	latest           bool
	generatedVersion bool
}

// NewMigrationsPushCommand returns the migrations push command.
func NewMigrationsPushCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "push <oci-reference>",
		Short: "Push a migration directory to an OCI registry",
		Long: `Push a migration directory to an OCI-compliant registry as an immutable
OCI 1.1 artifact.

The artifact is tagged with the tag the reference named and every explicit
--tag, and with nothing else. Moving the latest alias is a promotion rather than
a publication, so it is opt-in through --latest; a timestamped write-once
version tag is opt-in through --generated-version. A publish that did both by
default was performing two operations under one verb, and the second one is the
one an operator has to be able to decline.

The default output is a receipt for a person. --format reference prints only
the pinned reference of the manifest this push wrote, and --format json prints
it with the digest, artifact type, tags and version, so a script can carry the
artifact forward without reading the receipt:

    REF=$(ptah migrations push oci://ghcr.io/acme/app-migrations:v1 --format reference)
    ptah migrations up --db-url "$DATABASE_URL" --migrations-dir "$REF"

The reference comes from the manifest the push wrote, not from resolving a tag
afterwards. A push that fails prints nothing on standard output.

Authentication comes from "ptah oci login", the PTAH_OCI_USERNAME and
PTAH_OCI_PASSWORD environment variables, or Docker's credential configuration,
whichever holds a credential for the registry; --plain-http is intended only for
explicitly trusted local registries.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.migrationsDir, "migrations-dir", "./migrations", "Migration directory to publish")
	flags.StringArrayVar(&opts.tags, "tag", nil, "Additional movable tag to apply (repeatable)")
	flags.StringVar(&opts.version, "version", "", "Write-once version tag (defaults to v<UTC timestamp>)")
	flags.StringVar(&opts.dirFormat, "dir-format", string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.BoolVar(&opts.latest, "latest", false,
		"Also move the latest alias onto this push")
	flags.BoolVar(&opts.generatedVersion, "generated-version", false,
		"Also write a timestamped write-once version tag when --version names none")
	flags.BoolVar(&opts.verifySum, "verify-sum", false, migrationsource.VerifySumUsage(
		"Require the local migration directory to match its ptah.sum or atlas.sum before "+
			"pushing, and fail when it carries neither",
	))
	pushreceipt.RegisterFormatFlag(flags, &opts.format)
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	format, err := pushreceipt.ParseFormat(opts.format, reference)
	if err != nil {
		return err
	}
	dirFormat, err := migrationfile.ParseDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	result, err := migrationartifact.PushDirectory(cmd.Context(), migrationartifact.DirectoryPushOptions{
		Latest:           opts.latest,
		GeneratedVersion: opts.generatedVersion,
		Reference:        reference,
		Directory:        opts.migrationsDir,
		Tags:             opts.tags,
		Version:          opts.version,
		DirFormat:        dirFormat,
		PlainHTTP:        opts.plainHTTP,
		VerifySum:        opts.verifySum,
	})
	if err != nil {
		return err
	}
	receipt := fmt.Sprintf(
		"Pushed %s as %s\nDigest: %s\nVersion: %s\nTags: %v\n",
		result.Directory,
		result.Reference,
		result.Descriptor.Digest,
		result.Version,
		result.Tags,
	)
	return pushreceipt.Write(cmd.OutOrStdout(), format, result.PushResult, result.Version, receipt)
}
