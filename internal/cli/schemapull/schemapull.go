// Package schemapull implements desired-schema retrieval from OCI registries,
// Git repositories and HTTPS URLs.
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
	"ptah.run/internal/cli/internal/sourcepull"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
)

// skipParseFlag writes an OCI artifact without reading its schema.
const skipParseFlag = "skip-parse"

// ociOnlyFlags mean nothing for a Git or an HTTPS reference: its file is
// fetched over its own transport, and its bytes are written unchanged, so there
// is no parse to skip.
var ociOnlyFlags = []string{dbcli.PlainHTTPFlagName, skipParseFlag}

type options struct {
	output    string
	plainHTTP bool
	json      bool
	skipParse bool
	source    sourcepull.Flags
}

// NewSchemaPullCommand returns the schema pull command.
func NewSchemaPullCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "pull <oci-reference | git-reference | https-url>",
		Short: "Pull a desired schema from an OCI registry, a Git repository or an HTTPS URL",
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
--known-hosts-file.

An https:// reference names one download, which --expect-download-digest pins
by the SHA-256 of its exact bytes; nothing is read from it before that digest
matches. --format says whether the download is the schema file itself (file,
with --path the name it is given) or an archive (tar.gz or zip, with --path the
member to read). The file's bytes are written unchanged, and the download and
content digests are reported; with --expect-digest, other content is refused.
Basic authentication comes from PTAH_HTTPS_USERNAME and PTAH_HTTPS_PASSWORD, a
Bearer token from PTAH_HTTPS_TOKEN.`,
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
	sourcepull.Register(flags, &opts.source, sourcepull.File)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	source := sourcepull.SourceOf(reference)
	if err := sourcepull.RefuseFlags(cmd.Flags(), source); err != nil {
		return err
	}
	if source != sourcepull.OCI {
		for _, name := range ociOnlyFlags {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s is for an oci:// reference", name)
			}
		}
	}
	switch source {
	case sourcepull.Git:
		return pullGit(cmd, reference, opts)
	case sourcepull.HTTPS:
		return pullHTTPS(cmd, reference, opts)
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

func pullGit(cmd *cobra.Command, reference string, opts *options) error {
	result, err := sourcepull.FetchGit(cmd.Context(), reference, opts.source, sourcepull.File)
	if err != nil {
		return err
	}
	written, err := sourcepull.WriteFile(result.Files, opts.output)
	if err != nil {
		return err
	}
	if opts.json {
		return sourcepull.PrintGitJSON(cmd.OutOrStdout(), reference, opts.source.Path, result, written)
	}
	return sourcepull.PrintGitText(cmd.OutOrStdout(), reference, result, written)
}

func pullHTTPS(cmd *cobra.Command, reference string, opts *options) error {
	result, err := sourcepull.FetchHTTPS(cmd.Context(), reference, opts.source, sourcepull.File)
	if err != nil {
		return err
	}
	written, err := sourcepull.WriteFile(result.Files, opts.output)
	if err != nil {
		return err
	}
	if opts.json {
		return sourcepull.PrintHTTPSJSON(cmd.OutOrStdout(), reference, opts.source, result, written)
	}
	return sourcepull.PrintHTTPSText(cmd.OutOrStdout(), reference, result, written)
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
