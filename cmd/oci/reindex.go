package oci

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

type reindexOptions struct {
	format    string
	plainHTTP bool
}

type reindexRecord struct {
	Reference  string   `json:"reference"`
	Subject    string   `json:"subject"`
	Indexed    []string `json:"indexed,omitempty"`
	Repaired   []string `json:"repaired,omitempty"`
	Unrepaired []string `json:"unrepaired,omitempty"`
}

func newReindexCommand() *cobra.Command {
	opts := reindexOptions{}
	cmd := &cobra.Command{
		Use:   "reindex <oci-reference>",
		Short: "Republish attachments the registry's referrers index does not list",
		Long: `Republish attachments the registry's referrers index does not list.

A registry that gained the referrers index after Ptah published through its
durable tag holds attachments no other OCI client can find, and nothing about
the artifact says so. Republishing the manifest repairs it: the content is
byte-identical, so the digest does not move, and a registry serving the index
builds the entry when it receives a manifest carrying a subject.

The pass ends by asking the registry again, because a registry that accepted
the manifest and built no entry looks exactly like one that did until somebody
checks. Anything still missing is reported as unrepaired rather than counted as
fixed.

This is a separate verb rather than something a publish does. The repair
changes someone else's registry state, and a publish that silently rewrote
history would be doing work nobody asked for on artifacts they may not own.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReindex(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runReindex(cmd *cobra.Command, reference string, opts reindexOptions) error {
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	result, err := client.ReindexReferrers(cmd.Context(), reference)
	if err != nil {
		return err
	}
	record := reindexRecord{
		Reference:  result.Reference.String(),
		Subject:    result.Subject.Digest.String(),
		Indexed:    digests(result.Indexed),
		Repaired:   digests(result.Repaired),
		Unrepaired: digests(result.Unrepaired),
	}
	if err := writeReindex(cmd.OutOrStdout(), opts.format, record); err != nil {
		return err
	}
	if len(record.Unrepaired) > 0 {
		return fmt.Errorf("%d attachment(s) are still missing from the registry's referrers index",
			len(record.Unrepaired))
	}
	return nil
}

func digests(descriptors []ocispec.Descriptor) []string {
	out := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		out = append(out, descriptor.Digest.String())
	}
	return out
}

func writeReindex(w io.Writer, format string, record reindexRecord) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(record)
	}
	fmt.Fprintf(w, "Reference: %s\n", record.Reference)
	fmt.Fprintf(w, "Subject:   %s\n", record.Subject)
	fmt.Fprintf(w, "Indexed:   %d\n", len(record.Indexed))
	fmt.Fprintf(w, "Repaired:  %d\n", len(record.Repaired))
	for _, digest := range record.Repaired {
		fmt.Fprintf(w, "  %s\n", digest)
	}
	if len(record.Unrepaired) > 0 {
		fmt.Fprintf(w, "Still missing from the index: %d\n", len(record.Unrepaired))
		for _, digest := range record.Unrepaired {
			fmt.Fprintf(w, "  %s\n", digest)
		}
	}
	return nil
}
