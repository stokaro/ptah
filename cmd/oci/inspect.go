package oci

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

const noReferrersFlag = "no-referrers"

type inspectOptions struct {
	format      string
	noReferrers bool
	plainHTTP   bool
}

// layerRecord is one file an artifact carries.
type layerRecord struct {
	Name      string `json:"name,omitempty"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest"`
}

// subjectRecord is the artifact a referrer is attached to.
type subjectRecord struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size"`
}

// discoveredRecord is a referrer plus the mechanism that found it.
type discoveredRecord struct {
	Digest       string `json:"digest"`
	ArtifactType string `json:"artifact_type"`
	Size         int64  `json:"size"`
	Source       string `json:"source"`
}

// inspectRecord is the machine-readable answer of `oci inspect`.
type inspectRecord struct {
	Reference       string             `json:"reference"`
	PinnedReference string             `json:"pinned_reference"`
	Digest          string             `json:"digest"`
	MediaType       string             `json:"media_type"`
	Size            int64              `json:"size"`
	ArtifactType    string             `json:"artifact_type,omitempty"`
	Subject         *subjectRecord     `json:"subject,omitempty"`
	Annotations     map[string]string  `json:"annotations,omitempty"`
	Layers          []layerRecord      `json:"layers,omitempty"`
	Referrers       []discoveredRecord `json:"referrers,omitempty"`
	// ReferrerDiscovery summarizes which mechanisms answered for this
	// artifact. It is absent when referrer discovery was not run.
	ReferrerDiscovery string `json:"referrer_discovery,omitempty"`
}

func newInspectCommand() *cobra.Command {
	opts := inspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect <oci-reference>",
		Short: "Report what an OCI artifact declares, without downloading it",
		Long: `Report what an OCI artifact declares, without downloading its files.

Reads the manifest and stops there: the descriptor, the artifact type, the
subject when the artifact is itself a referrer, the annotations, and each file
layer's name, media type, size and digest.

Referrer discovery is reported too, and the source column is the part worth
reading. Ptah writes both the standard referrers index and its own
content-derived durable tag. A referrer listed as ` + "`durable-tag`" + ` was returned by
the second mechanism only, which means Ptah discovers it and another OCI client
may not. Pass --` + noReferrersFlag + ` to skip that lookup.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInspect(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	flags.BoolVar(&opts.noReferrers, noReferrersFlag, false, "Skip referrer discovery")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runInspect(cmd *cobra.Command, reference string, opts inspectOptions) error {
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	info, err := client.Inspect(cmd.Context(), reference)
	if err != nil {
		return err
	}
	record := newInspectRecord(info)
	if !opts.noReferrers {
		_, discovered, err := client.DiscoverReferrers(cmd.Context(), reference, "")
		if err != nil {
			return err
		}
		record.Referrers = newDiscoveredRecords(discovered)
		record.ReferrerDiscovery = summarizeDiscovery(discovered)
	}
	return writeInspect(cmd.OutOrStdout(), opts.format, record)
}

func newInspectRecord(info ociartifact.ManifestInfo) inspectRecord {
	record := inspectRecord{
		Reference:       info.Reference.String(),
		PinnedReference: info.Reference.PinnedString(info.Descriptor.Digest.String()),
		Digest:          info.Descriptor.Digest.String(),
		MediaType:       info.Descriptor.MediaType,
		Size:            info.Descriptor.Size,
		ArtifactType:    info.ArtifactType,
		Annotations:     info.Annotations,
		Layers:          make([]layerRecord, 0, len(info.Layers)),
	}
	if info.Subject != nil {
		record.Subject = &subjectRecord{
			Digest:    info.Subject.Digest.String(),
			MediaType: info.Subject.MediaType,
			Size:      info.Subject.Size,
		}
	}
	for _, layer := range info.Layers {
		record.Layers = append(record.Layers, layerRecord{
			Name:      ociartifact.LayerName(layer),
			MediaType: layer.MediaType,
			Size:      layer.Size,
			Digest:    layer.Digest.String(),
		})
	}
	return record
}

func newDiscoveredRecords(discovered []ociartifact.DiscoveredReferrer) []discoveredRecord {
	records := make([]discoveredRecord, 0, len(discovered))
	for _, item := range discovered {
		records = append(records, discoveredRecord{
			Digest:       item.Descriptor.Digest.String(),
			ArtifactType: item.Descriptor.ArtifactType,
			Size:         item.Descriptor.Size,
			Source:       string(item.Source),
		})
	}
	return records
}

// summarizeDiscovery names the weakest guarantee any referrer here carries.
//
// The weakest rather than the most common one: a subject whose referrers are
// mostly in the standard index still has a cross-client discovery problem if
// one of them is not, and a summary that reported the majority would hide
// exactly the referrer an operator needs to hear about.
func summarizeDiscovery(discovered []ociartifact.DiscoveredReferrer) string {
	if len(discovered) == 0 {
		return "none"
	}
	if slices.ContainsFunc(discovered, func(item ociartifact.DiscoveredReferrer) bool {
		return item.Source == ociartifact.ReferrerSourceDurableTag
	}) {
		return string(ociartifact.ReferrerSourceDurableTag)
	}
	if slices.ContainsFunc(discovered, func(item ociartifact.DiscoveredReferrer) bool {
		return item.Source == ociartifact.ReferrerSourceAPI
	}) {
		return string(ociartifact.ReferrerSourceAPI)
	}
	return string(ociartifact.ReferrerSourceBoth)
}

func writeInspect(w io.Writer, format string, record inspectRecord) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(record)
	}
	return writeInspectText(w, record)
}

func writeInspectText(w io.Writer, record inspectRecord) error {
	fmt.Fprintf(w, "Reference:     %s\n", record.Reference)
	fmt.Fprintf(w, "Digest:        %s\n", record.Digest)
	fmt.Fprintf(w, "Media type:    %s\n", record.MediaType)
	fmt.Fprintf(w, "Size:          %d\n", record.Size)
	if record.ArtifactType != "" {
		fmt.Fprintf(w, "Artifact type: %s\n", record.ArtifactType)
	}
	if record.Subject != nil {
		fmt.Fprintf(w, "Subject:       %s\n", record.Subject.Digest)
	}
	if record.ReferrerDiscovery != "" {
		fmt.Fprintf(w, "Discovery:     %s\n", record.ReferrerDiscovery)
	}

	if len(record.Annotations) > 0 {
		fmt.Fprintln(w, "\nAnnotations:")
		writer := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, key := range slices.Sorted(maps.Keys(record.Annotations)) {
			fmt.Fprintf(writer, "  %s\t%s\n", key, record.Annotations[key])
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if len(record.Layers) > 0 {
		fmt.Fprintln(w, "\nFiles:")
		writer := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "  NAME\tMEDIA TYPE\tSIZE\tDIGEST")
		for _, layer := range record.Layers {
			fmt.Fprintf(writer, "  %s\t%s\t%d\t%s\n", layer.Name, layer.MediaType, layer.Size, layer.Digest)
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if len(record.Referrers) > 0 {
		fmt.Fprintln(w, "\nReferrers:")
		writer := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "  ARTIFACT TYPE\tSIZE\tSOURCE\tDIGEST")
		for _, referrer := range record.Referrers {
			fmt.Fprintf(writer, "  %s\t%d\t%s\t%s\n",
				referrer.ArtifactType, referrer.Size, referrer.Source, referrer.Digest)
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return nil
}
