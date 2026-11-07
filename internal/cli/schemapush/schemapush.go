// Package schemapush implements desired-schema publication to OCI registries.
package schemapush

import (
	"bytes"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/pushreceipt"
	"ptah.run/internal/schemaartifact"
	"ptah.run/internal/schemaload"
	"ptah.run/internal/schemaroot"
)

type options struct {
	rootDirs    []string
	schemaFiles []string
	dialect     string
	tags        []string
	version     string
	checks      string
	plainHTTP   bool
	format      string

	latest           bool
	generatedVersion bool
}

// NewSchemaPushCommand returns the schema push command.
func NewSchemaPushCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "push <oci-reference>",
		Short: "Push a desired schema to an OCI registry",
		Long: `Resolve a desired schema from Go annotations, YAML files, HCL files, or
SQL files and publish a lossless canonical HCL representation as an immutable
OCI 1.1 artifact.

The artifact is tagged with the tag the reference named and every explicit
--tag, and with nothing else. Moving the latest alias is a promotion rather than
a publication, so it is opt-in through --latest, and a timestamped write-once
version tag is opt-in through --generated-version.

The default output is a receipt for a person. --format reference prints only
the pinned reference of the manifest this push wrote, and --format json prints
it with the digest, artifact type, tags and version, so a script can carry the
artifact forward without reading the receipt:

    REF=$(ptah schema push oci://ghcr.io/acme/app-schema:v1 --schema-file schema.sql --format reference)

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
	flags.StringArrayVar(&opts.rootDirs, "root-dir", nil, "Root directory to scan for Go entities (repeatable; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, "schema-file", nil, "SQL, YAML, HCL, or DBML desired-schema file (repeatable)")
	flags.StringVar(&opts.dialect, "dialect", "", "Dialect hint used when parsing SQL schema files")
	flags.StringArrayVar(&opts.tags, "tag", nil, "Additional movable tag to apply (repeatable)")
	flags.StringVar(&opts.version, "version", "", "Write-once version tag (defaults to v<UTC timestamp>)")
	flags.StringVar(&opts.checks, checksFlag, "",
		"`path` to a .sql file of \"-- +ptah check\" release assertions published with the schema")
	flags.BoolVar(&opts.latest, "latest", false,
		"Also move the latest alias onto this push")
	flags.BoolVar(&opts.generatedVersion, "generated-version", false,
		"Also write a timestamped write-once version tag when --version names none")
	pushreceipt.RegisterFormatFlag(flags, &opts.format)
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	format, err := pushreceipt.ParseFormat(opts.format, reference)
	if err != nil {
		return err
	}
	declaredVars, err := dbcli.DeclaredVars(cmd)
	if err != nil {
		return err
	}
	owners, err := engineruntime.Resolve(cmd.Context())
	if err != nil {
		return err
	}
	db, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    opts.rootDirs,
		Owners:      owners,
		SchemaFiles: opts.schemaFiles,
		Dialect:     opts.dialect,
		PlainHTTP:   opts.plainHTTP,
		Vars:        declaredVars,
	})
	if err != nil {
		return err
	}
	// An artifact travels without the working copy that declared it, so the
	// declared rows are read here, where the working copy is still reachable,
	// rather than wherever the artifact is later read.
	if err := schemaload.ReadManagedRows(db, schemaroot.Of(opts.rootDirs)); err != nil {
		return err
	}
	checks, err := readChecks(opts.checks)
	if err != nil {
		return err
	}
	result, err := schemaartifact.Push(cmd.Context(), reference, db, schemaartifact.PushOptions{
		Checks:           checks,
		Latest:           opts.latest,
		GeneratedVersion: opts.generatedVersion,
		Tags:             opts.tags,
		Version:          opts.version,
		PlainHTTP:        opts.plainHTTP,
		// The snapshot is read back with the vocabulary of the owners that
		// loaded it, which is what its header may name.
		CoverageVocabulary: owners.CoverageVocabulary(),
		HCL:                owners.HCL(),
	})
	if err != nil {
		return err
	}
	receipt := fmt.Sprintf(
		"Pushed schema as %s\nDigest: %s\nVersion: %s\nTags: %v\n",
		result.Reference,
		result.Descriptor.Digest,
		result.Version,
		result.Tags,
	)
	return pushreceipt.Write(cmd.OutOrStdout(), format, result.PushResult, result.Version, receipt)
}

// checksFlag names the release assertions published beside the schema.
const checksFlag = "checks"

// readChecks reads the assertions to publish, and refuses a file that carries
// none.
//
// An empty file is a mistake worth naming: the operator asked for a checks
// layer, and publishing an artifact whose layer asserts nothing would make
// `ptah db verify` against it report a clean release it never verified.
//
// The file is not parsed here. Parsing needs the dialect of the database the
// assertions will run against, which a publish does not have and must not
// guess: the same artifact is verified against every database of a fleet.
func readChecks(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	source, err := os.ReadFile(path) // #nosec G304 -- the operator named this file
	if err != nil {
		return nil, fmt.Errorf("read checks file %s: %w", path, err)
	}
	if len(bytes.TrimSpace(source)) == 0 {
		return nil, fmt.Errorf("checks file %s is empty, so the artifact would assert nothing", path)
	}
	return source, nil
}
