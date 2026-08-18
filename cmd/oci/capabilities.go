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

type capabilitiesOptions struct {
	format    string
	plainHTTP bool
}

type capabilitiesRecord struct {
	Reference    string `json:"reference"`
	ReferrersAPI bool   `json:"referrers_api"`
	Detail       string `json:"detail,omitempty"`
}

func newCapabilitiesCommand() *cobra.Command {
	opts := capabilitiesOptions{}
	cmd := &cobra.Command{
		Use:   "capabilities <oci-reference>",
		Short: "Report what the registry behind a reference supports",
		Long: `Report what the registry behind a reference supports.

Ptah publishes referrers two ways — the standard index and its own
content-derived tag — and merges them on read. That makes Ptah's own discovery
robust and says nothing about anyone else's: where the registry has no
referrers API, a referrer Ptah published is one another OCI client may never
find.

This asks the registry directly rather than inferring it. The question is put
by making the request with the client pinned to the API, so it cannot quietly
fall back to the tag schema and report a success that came from somewhere else.
A refusal naming the API as unsupported is the registry saying no; anything
else is a failure to ask, and is reported as an error rather than as a no.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCapabilities(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runCapabilities(cmd *cobra.Command, reference string, opts capabilitiesOptions) error {
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	capabilities, err := client.Capabilities(cmd.Context(), reference)
	if err != nil {
		return err
	}
	record := capabilitiesRecord{
		Reference:    capabilities.Reference.String(),
		ReferrersAPI: capabilities.ReferrersAPI,
		Detail:       capabilities.Detail,
	}
	return writeCapabilities(cmd.OutOrStdout(), opts.format, record)
}

func writeCapabilities(w io.Writer, format string, record capabilitiesRecord) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(record)
	}
	fmt.Fprintf(w, "Reference:     %s\n", record.Reference)
	fmt.Fprintf(w, "Referrers API: %t\n", record.ReferrersAPI)
	if record.Detail != "" {
		fmt.Fprintf(w, "Detail:        %s\n", record.Detail)
	}
	if !record.ReferrersAPI {
		fmt.Fprintln(w, "\nReferrers Ptah publishes here are discoverable through its durable tag.")
		fmt.Fprintln(w, "Another OCI client may not find them.")
	}
	return nil
}
