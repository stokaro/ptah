package oci

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

type resolveOptions struct {
	format    string
	plainHTTP bool
}

// resolveRecord is the machine-readable answer of `oci resolve`.
type resolveRecord struct {
	Reference       string `json:"reference"`
	PinnedReference string `json:"pinned_reference"`
	Digest          string `json:"digest"`
	MediaType       string `json:"media_type"`
	Size            int64  `json:"size"`
}

func newResolveCommand() *cobra.Command {
	opts := resolveOptions{}
	cmd := &cobra.Command{
		Use:   "resolve <oci-reference>",
		Short: "Resolve a mutable tag to the immutable digest it names",
		Long: `Resolve a mutable tag to the immutable digest it names.

The text output is the pinned reference alone, so a CI step can capture it and
pass it to the verbs that consume an artifact:

    DIGEST=$(ptah oci resolve ghcr.io/acme/db:latest)
    ptah migrations up --from "$DIGEST"

Pinning is what makes the two steps describe the same artifact. A tag moved
between them would otherwise change what runs without changing the pipeline.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runResolve(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runResolve(cmd *cobra.Command, reference string, opts resolveOptions) error {
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	ref, descriptor, err := client.Resolve(cmd.Context(), reference)
	if err != nil {
		return err
	}
	record := resolveRecord{
		Reference:       ref.String(),
		PinnedReference: ref.PinnedString(descriptor.Digest.String()),
		Digest:          descriptor.Digest.String(),
		MediaType:       descriptor.MediaType,
		Size:            descriptor.Size,
	}
	return writeResolve(cmd.OutOrStdout(), opts.format, record)
}

func writeResolve(w io.Writer, format string, record resolveRecord) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(record)
	}
	_, err := fmt.Fprintln(w, record.PinnedReference)
	return err
}
