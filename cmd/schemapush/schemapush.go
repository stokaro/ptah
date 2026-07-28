// Package schemapush implements desired-schema publication to OCI registries.
package schemapush

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/schemaload"
	"github.com/stokaro/ptah/internal/schemaartifact"
)

type options struct {
	rootDirs    []string
	schemaFiles []string
	dialect     string
	tags        []string
	version     string
	plainHTTP   bool
}

// NewSchemaPushCommand returns the schema push command.
func NewSchemaPushCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "push <oci-reference>",
		Short: "Push a desired schema to an OCI registry",
		Long: `Resolve a desired schema from Go annotations, YAML files, HCL files, or
SQL files and publish a lossless canonical HCL representation as an immutable
OCI 1.1 artifact. Authentication comes from the Docker credential store.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, "root-dir", nil, "Root directory to scan for Go entities (repeatable; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, "schema-file", nil, "YAML, HCL, or SQL desired-schema file (repeatable)")
	flags.StringVar(&opts.dialect, "dialect", "", "Dialect hint used when parsing SQL schema files")
	flags.StringArrayVar(&opts.tags, "tag", nil, "Additional movable tag to apply (repeatable)")
	flags.StringVar(&opts.version, "version", "", "Write-once version tag (defaults to v<UTC timestamp>)")
	flags.BoolVar(&opts.plainHTTP, "plain-http", false, "Use plain HTTP for an explicitly trusted local registry")
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	db, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Dialect:     opts.dialect,
		PlainHTTP:   opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	result, err := schemaartifact.Push(cmd.Context(), reference, db, schemaartifact.PushOptions{
		Tags:      opts.tags,
		Version:   opts.version,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"Pushed schema as %s\nDigest: %s\nVersion: %s\nTags: %v\n",
		result.Reference,
		result.Descriptor.Digest,
		result.Version,
		result.Tags,
	)
	return nil
}
