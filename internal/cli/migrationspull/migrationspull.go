// Package migrationspull implements migration directory retrieval from OCI
// registries, Git repositories and HTTPS URLs.
package migrationspull

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/sourcepull"
	"ptah.run/internal/migrationartifact"
)

type options struct {
	output    string
	plainHTTP bool
	json      bool
	source    sourcepull.Flags
}

// NewMigrationsPullCommand returns the migrations pull command.
func NewMigrationsPullCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "pull <oci-reference | git-reference | https-url>",
		Short: "Pull a migration directory from an OCI registry, a Git repository or an HTTPS URL",
		Long: `Pull a migration directory and write it to a new local directory.

An oci:// reference names a Ptah migration artifact, which is validated and
reconstructed byte-for-byte. Authentication comes from "ptah oci login", the
PTAH_OCI_USERNAME and PTAH_OCI_PASSWORD environment variables, or Docker's
credential configuration, whichever holds a credential for the registry;
--plain-http is intended only for explicitly trusted local registries.

A git+https:// or git+ssh:// reference names a repository. --branch, --tag or
--commit says which commit, and --path the migration directory in its tree
("." for the root); every file under it is written at its path relative to it.
One commit is fetched shallowly and only that path is read: no working tree,
hook, filter, submodule or LFS step. The commit and the content digest are
reported; with --commit and --expect-digest, other content is refused. HTTPS
authenticates with PTAH_GIT_USERNAME and PTAH_GIT_PASSWORD (a token goes in
the password), SSH with the unencrypted key PTAH_GIT_SSH_KEY_FILE names, and
SSH requires --known-hosts-file.

An https:// reference names one archive, which --expect-download-digest pins
by the SHA-256 of its exact bytes; nothing is read from it before that digest
matches. --format is tar.gz or zip, and --path the directory in it ("." for the
root); every file under it is written at its path relative to it. Every entry
name is checked, and inside the directory only regular files and directories
are read. The download and content digests are reported; with --expect-digest,
other content is refused. Basic authentication comes from PTAH_HTTPS_USERNAME
and PTAH_HTTPS_PASSWORD, a Bearer token from PTAH_HTTPS_TOKEN.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))

	flags := cmd.Flags()
	flags.StringVar(&opts.output, "out", "", "Output directory to create (required)")
	flags.BoolVar(&opts.json, "json", false, "Print what was pulled as one JSON document")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	sourcepull.Register(flags, &opts.source, sourcepull.Directory)
	return cmd
}

func run(cmd *cobra.Command, reference string, opts *options) error {
	source := sourcepull.SourceOf(reference)
	if err := sourcepull.RefuseFlags(cmd.Flags(), source); err != nil {
		return err
	}
	if source != sourcepull.OCI && cmd.Flags().Changed(dbcli.PlainHTTPFlagName) {
		return fmt.Errorf("--%s is for an oci:// reference", dbcli.PlainHTTPFlagName)
	}
	switch source {
	case sourcepull.Git:
		return pullGit(cmd, reference, opts)
	case sourcepull.HTTPS:
		return pullHTTPS(cmd, reference, opts)
	}
	result, err := migrationartifact.PullDirectory(cmd.Context(), migrationartifact.DirectoryPullOptions{
		Reference: reference,
		Output:    opts.output,
		PlainHTTP: opts.plainHTTP,
	})
	if err != nil {
		return err
	}
	if opts.json {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(ociReport{
			Source: "oci", Reference: result.Artifact.Reference.String(), Digest: result.Artifact.Descriptor.Digest.String(),
			Format: string(result.Artifact.DirFormat), Written: []string{result.Output},
		})
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

func pullGit(cmd *cobra.Command, reference string, opts *options) error {
	result, err := sourcepull.FetchGit(cmd.Context(), reference, opts.source, sourcepull.Directory)
	if err != nil {
		return err
	}
	written, err := sourcepull.WriteDirectory(result.Files, opts.output)
	if err != nil {
		return err
	}
	if opts.json {
		return sourcepull.PrintGitJSON(cmd.OutOrStdout(), reference, opts.source.Path, result, written)
	}
	return sourcepull.PrintGitText(cmd.OutOrStdout(), reference, result, written)
}

func pullHTTPS(cmd *cobra.Command, reference string, opts *options) error {
	result, err := sourcepull.FetchHTTPS(cmd.Context(), reference, opts.source, sourcepull.Directory)
	if err != nil {
		return err
	}
	written, err := sourcepull.WriteDirectory(result.Files, opts.output)
	if err != nil {
		return err
	}
	if opts.json {
		return sourcepull.PrintHTTPSJSON(cmd.OutOrStdout(), reference, opts.source, result, written)
	}
	return sourcepull.PrintHTTPSText(cmd.OutOrStdout(), reference, result, written)
}

// ociReport is the --json document of an OCI pull.
type ociReport struct {
	Source    string   `json:"source"`
	Reference string   `json:"reference"`
	Digest    string   `json:"digest"`
	Format    string   `json:"format"`
	Written   []string `json:"written"`
}
