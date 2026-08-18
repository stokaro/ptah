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

type tagsOptions struct {
	format    string
	plainHTTP bool
}

func newTagsCommand() *cobra.Command {
	opts := tagsOptions{}
	cmd := &cobra.Command{
		Use:   "tags <oci-reference>",
		Short: "List the tags a repository carries",
		Long: `List the tags a repository carries.

The aliases are what a promotion moves and what a pin replaces, so this is the
view that says which of them currently exist and, with ` + "`oci resolve`" + `, which
manifest each one names.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTags(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runTags(cmd *cobra.Command, reference string, opts tagsOptions) error {
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	tags, err := client.Tags(cmd.Context(), reference)
	if err != nil {
		return err
	}
	return writeTags(cmd.OutOrStdout(), opts.format, ociartifact.SortedTags(tags))
}

func writeTags(w io.Writer, format string, tags []string) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(tags)
	}
	for _, tag := range tags {
		if _, err := fmt.Fprintln(w, tag); err != nil {
			return err
		}
	}
	return nil
}
