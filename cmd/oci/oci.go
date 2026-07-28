// Package oci contains commands for inspecting Ptah OCI artifacts.
package oci

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/internal/ociartifact"
	"github.com/stokaro/ptah/internal/ocireferrers"
)

const (
	typeFlag      = "type"
	formatFlag    = "format"
	plainHTTPFlag = "plain-http"
)

type referrerOptions struct {
	filter    string
	format    string
	plainHTTP bool
}

// NewCommand returns the OCI artifact-inspection namespace.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "oci",
		Short: "Inspect Ptah artifacts in OCI registries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newReferrersCommand())
	return cmd
}

func newReferrersCommand() *cobra.Command {
	opts := referrerOptions{}
	cmd := &cobra.Command{
		Use:   "referrers <oci-reference>",
		Short: "List metadata attached to an OCI artifact",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReferrers(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.filter, typeFlag, ocireferrers.FilterAll, "Referrer type: all, lint, plan, or deployment")
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	flags.BoolVar(&opts.plainHTTP, plainHTTPFlag, false, "Allow an unencrypted HTTP connection to a local OCI registry")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runReferrers(cmd *cobra.Command, reference string, opts referrerOptions) error {
	if _, err := ocireferrers.ArtifactType(opts.filter); err != nil {
		return err
	}
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	records, err := ocireferrers.List(cmd.Context(), client, reference, opts.filter)
	if err != nil {
		return fmt.Errorf("list OCI referrers: %w", err)
	}
	return ocireferrers.Write(cmd.OutOrStdout(), strings.ToLower(strings.TrimSpace(opts.format)), records)
}
