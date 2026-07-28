// Package lintartifact publishes migration lint reports as OCI referrers.
package lintartifact

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migrationlintreport"
	"github.com/stokaro/ptah/internal/ociartifact"
	migrationlint "github.com/stokaro/ptah/migration/lint"
)

const (
	// FileName is the artifact layer name used for migration lint reports.
	FileName = "lint.json"

	// LayerMediaType identifies a migration lint report JSON layer.
	LayerMediaType = "application/vnd.stokaro.ptah.migration.lint.report.v1+json"
)

// NewFS returns an immutable filesystem containing the canonical JSON report.
func NewFS(report migrationlintreport.Report) (fs.FS, error) {
	if report.Findings == nil {
		report.Findings = []migrationlint.Finding{}
	}

	var contents bytes.Buffer
	if err := migrationlintreport.Write(
		&contents,
		migrationlintreport.FormatJSON,
		report,
	); err != nil {
		return nil, fmt.Errorf("render migration lint report: %w", err)
	}

	snapshot, err := fsnapshot.FromFiles(map[string][]byte{FileName: contents.Bytes()})
	if err != nil {
		return nil, fmt.Errorf("build migration lint report filesystem: %w", err)
	}
	return snapshot, nil
}

// Publish attaches report to an already resolved migration artifact.
func Publish(
	ctx context.Context,
	client *ociartifact.Client,
	subjectRef string,
	subject ocispec.Descriptor,
	report migrationlintreport.Report,
) (ociartifact.PushResult, error) {
	if client == nil {
		return ociartifact.PushResult{}, fmt.Errorf("OCI client is required")
	}
	reportFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return client.AttachResolved(ctx, subjectRef, subject, reportFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.LintArtifactType,
		LayerMediaType: LayerMediaType,
	})
}

// PublishTo attaches report to subject in target.
func PublishTo(
	ctx context.Context,
	target oras.Target,
	subject ocispec.Descriptor,
	report migrationlintreport.Report,
) (ociartifact.PushResult, error) {
	reportFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return ociartifact.AttachTo(ctx, target, subject, reportFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.LintArtifactType,
		LayerMediaType: LayerMediaType,
	})
}
