// Package schemapull implements desired-schema retrieval from OCI registries
// and Git repositories.
package schemapull

import (
	"encoding/json"
	"fmt"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/gitpull"
	"ptah.run/internal/gitsource"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
)

// skipParseFlag writes an OCI artifact without reading its schema.
const skipParseFlag = "skip-parse"

// ociOnlyFlags mean nothing for a Git reference: its file is fetched over Git's
// own transport, and its bytes are written unchanged, so there is no parse to
// skip.
var ociOnlyFlags = []string{dbcli.PlainHTTPFlagName, skipParseFlag}

type options struct {
	output    string
	plainHTTP bool
	json      bool
	skipParse bool
	git       gitpull.Flags
}

// NewSchemaPullCommand returns the schema pull command.
func NewSchemaPullCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "pull <oci-reference | git-reference>",
		Short: "Pull a desired schema from an OCI registry or a Git repository",
		Long: `Pull a desired schema and write it to a new local file.

An oci:// reference names a Ptah desired-schema artifact, which is validated
and written as canonical HCL. Authentication comes from the Docker credential
store.

--skip-parse writes an oci:// artifact without reading its schema, for a step
that holds registry credentials and must not report what an artifact says. The
manifest, artifact type, format, layer media types, sizes, digests and file names
are still checked. The next command that reads the file reports what it cannot
read. The schema goes to --out, which has to end in .hcl, and managed-data.json is
always written beside it, empty when the artifact carries no rows.

A git+https:// or git+ssh:// reference names a repository. --branch, --tag or
--commit says which commit, and --path the schema file in its tree; the file's
bytes are written unchanged, so --out should keep its extension. One commit is
fetched shallowly and only that path is read: no working tree, hook, filter,
submodule or LFS step. The commit and the content digest are reported; with
--commit and --expect-digest, other content is refused. HTTPS authenticates
with PTAH_GIT_USERNAME and PTAH_GIT_PASSWORD (a token goes in the password),
SSH with the unencrypted key PTAH_GIT_SSH_KEY_FILE names, and SSH requires
--known-hosts-file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.output, "out", "", "Output file to create (required)")
	flags.BoolVar(&opts.json, "json", false, "Print what was pulled as one JSON document")
	flags.BoolVar(&opts.skipParse, skipParseFlag, false,
		"OCI: write the artifact without reading its schema; the next command that reads the file reports what it cannot read")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	gitpull.Register(flags, &opts.git, "schema file")
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	if gitsource.IsReference(reference) {
		for _, name := range ociOnlyFlags {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s is for an oci:// reference", name)
			}
		}
		result, err := gitpull.Fetch(cmd.Context(), reference, opts.git, gitsource.File)
		if err != nil {
			return err
		}
		written, err := gitpull.WriteFile(result, opts.output)
		if err != nil {
			return err
		}
		if opts.json {
			return gitpull.PrintJSON(cmd.OutOrStdout(), reference, opts.git.Path, result, written)
		}
		return gitpull.PrintText(cmd.OutOrStdout(), reference, result, written)
	}
	if err := gitpull.RefuseOnOtherSource(cmd.Flags()); err != nil {
		return err
	}
	// An unparsed pull resolves no runtime: the runtime is what a parse reads the
	// schema with.
	if opts.skipParse {
		artifact, written, err := schemaartifact.PullUnparsedToFile(cmd.Context(), reference, opts.output, opts.plainHTTP)
		if err != nil {
			return err
		}
		return printOCIReport(cmd, opts, artifact.Reference, artifact.Descriptor, written)
	}
	owners, err := engineruntime.Resolve(cmd.Context())
	if err != nil {
		return err
	}
	artifact, written, err := schemaartifact.PullToFile(
		cmd.Context(),
		reference,
		opts.output,
		opts.plainHTTP,
		owners.CoverageVocabulary(),
		owners.HCL(),
	)
	if err != nil {
		return err
	}
	return printOCIReport(cmd, opts, artifact.Reference, artifact.Descriptor, written)
}

func printOCIReport(cmd *cobra.Command, opts *options, reference ociartifact.Reference, descriptor ocispec.Descriptor, written []string) error {
	if opts.json {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(ociReport{
			Source: "oci", Reference: reference.String(), Digest: descriptor.Digest.String(), Written: written, Unparsed: opts.skipParse,
		})
	}
	// Every path is printed, not only the one --out named. An artifact that
	// declares rows is materialized as two files, and a person who is told
	// about one of them has no reason to keep the other.
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"Pulled %s to %s\nDigest: %s\n",
		reference,
		strings.Join(written, ", "),
		descriptor.Digest,
	)
	if opts.skipParse {
		fmt.Fprintln(cmd.OutOrStdout(), "Not parsed: the next command that reads the file reports what it cannot read.")
	}
	return nil
}

// ociReport is the --json document of an OCI pull. Unparsed is present only on
// a pull that did not read the schema.
type ociReport struct {
	Source    string   `json:"source"`
	Reference string   `json:"reference"`
	Digest    string   `json:"digest"`
	Written   []string `json:"written"`
	Unparsed  bool     `json:"unparsed,omitempty"`
}
