// Package ocireferrers lists and renders metadata attached to OCI artifacts.
package ocireferrers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/ociartifact"
)

const (
	// FilterAll includes every direct referrer.
	FilterAll = "all"
	// FilterLint includes migration lint reports.
	FilterLint = "lint"
	// FilterPlan includes migration plans.
	FilterPlan = "plan"
	// FilterDeployment includes migration deployment reports.
	FilterDeployment = "deployment"

	// FormatText renders a human-readable table.
	FormatText = "text"
	// FormatJSON renders a machine-readable JSON array.
	FormatJSON = "json"
)

// Record is the stable machine-readable representation of a referrer.
type Record struct {
	Digest       string            `json:"digest"`
	ArtifactType string            `json:"artifact_type"`
	MediaType    string            `json:"media_type"`
	Size         int64             `json:"size"`
	Annotations  map[string]string `json:"annotations,omitempty"`
}

// ArtifactType converts a user-facing filter into its OCI artifact type.
func ArtifactType(filter string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(filter)) {
	case FilterAll:
		return "", nil
	case FilterLint:
		return ociartifact.LintArtifactType, nil
	case FilterPlan:
		return ociartifact.PlanArtifactType, nil
	case FilterDeployment:
		return ociartifact.DeploymentArtifactType, nil
	default:
		return "", fmt.Errorf("unsupported referrer type %q: expected all, lint, plan, or deployment", filter)
	}
}

// ValidateFormat validates an output format.
func ValidateFormat(format string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case FormatText, FormatJSON:
		return nil
	default:
		return fmt.Errorf("unsupported output format %q: expected text or json", format)
	}
}

// List returns sorted direct referrers for reference.
func List(
	ctx context.Context,
	client *ociartifact.Client,
	reference,
	filter string,
) ([]Record, error) {
	if client == nil {
		return nil, fmt.Errorf("OCI client is required")
	}
	artifactType, err := ArtifactType(filter)
	if err != nil {
		return nil, err
	}
	descriptors, err := client.Referrers(ctx, reference, artifactType)
	if err != nil {
		return nil, err
	}
	return NewRecords(descriptors), nil
}

// NewRecords converts descriptors into stable sorted records.
func NewRecords(descriptors []ocispec.Descriptor) []Record {
	records := make([]Record, 0, len(descriptors))
	for _, descriptor := range descriptors {
		records = append(records, Record{
			Digest:       descriptor.Digest.String(),
			ArtifactType: descriptor.ArtifactType,
			MediaType:    descriptor.MediaType,
			Size:         descriptor.Size,
			Annotations:  maps.Clone(descriptor.Annotations),
		})
	}
	slices.SortFunc(records, func(left, right Record) int {
		if order := strings.Compare(left.ArtifactType, right.ArtifactType); order != 0 {
			return order
		}
		return strings.Compare(left.Digest, right.Digest)
	})
	return records
}

// Write renders records in format.
func Write(w io.Writer, format string, records []Record) error {
	if err := ValidateFormat(format); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case FormatText:
		return writeText(w, records)
	case FormatJSON:
		return writeJSON(w, records)
	}
	return nil
}

func writeText(w io.Writer, records []Record) error {
	if len(records) == 0 {
		_, err := fmt.Fprintln(w, "No referrers found.")
		return err
	}
	if _, err := fmt.Fprintln(w, "DIGEST\tARTIFACT TYPE\tMEDIA TYPE\tSIZE"); err != nil {
		return err
	}
	for _, record := range records {
		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%d\n",
			escapeText(record.Digest),
			escapeText(record.ArtifactType),
			escapeText(record.MediaType),
			record.Size,
		); err != nil {
			return err
		}
	}
	return nil
}

func escapeText(value string) string {
	quoted := strconv.QuoteToGraphic(value)
	return quoted[1 : len(quoted)-1]
}

func writeJSON(w io.Writer, records []Record) error {
	if records == nil {
		records = make([]Record, 0)
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(records)
}
