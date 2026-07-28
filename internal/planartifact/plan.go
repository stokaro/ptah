// Package planartifact builds deterministic migration-plan OCI attachments.
package planartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"slices"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"github.com/stokaro/ptah/core/platform/capability"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/ociartifact"
	"github.com/stokaro/ptah/migration/safety"
)

const (
	// FileName is the artifact layer name used for migration plans.
	FileName = "plan.json"

	// LayerMediaType identifies a migration safety-plan JSON layer.
	LayerMediaType = "application/vnd.stokaro.ptah.migration.plan.v1+json"

	// SchemaVersion identifies the plan artifact JSON contract.
	SchemaVersion = 1
)

// Report binds a generated migration plan to both its desired OCI artifact and
// the live current schema used to derive it.
type Report struct {
	SchemaVersion         int                          `json:"schema_version"`
	DesiredArtifactDigest string                       `json:"desired_artifact_digest"`
	CurrentSchemaDigest   string                       `json:"current_schema_digest"`
	Dialect               string                       `json:"dialect"`
	Schemas               []string                     `json:"schemas"`
	Capabilities          capability.Capabilities      `json:"capabilities"`
	Assessments           []safety.StatementAssessment `json:"assessments"`
}

// NewReport constructs a deterministic report bound to current and desired
// state. The current-state digest hashes the complete schema IR.
func NewReport(
	subject ocispec.Descriptor,
	current *dbtypes.DBSchema,
	dialect string,
	capabilities capability.Capabilities,
	schemas []string,
	assessments []safety.StatementAssessment,
) (Report, error) {
	if subject.Digest == "" {
		return Report{}, fmt.Errorf("desired schema artifact digest is required")
	}
	if current == nil {
		return Report{}, fmt.Errorf("current schema is required")
	}
	currentJSON, err := json.Marshal(current)
	if err != nil {
		return Report{}, fmt.Errorf("marshal current schema fingerprint input: %w", err)
	}
	schemas = slices.Clone(schemas)
	slices.Sort(schemas)
	schemas = slices.Compact(schemas)
	if schemas == nil {
		schemas = []string{}
	}
	if capabilities == nil {
		capabilities = capability.Capabilities{}
	} else {
		capabilities = capabilities.Clone()
	}
	if assessments == nil {
		assessments = []safety.StatementAssessment{}
	} else {
		assessments = slices.Clone(assessments)
	}
	return Report{
		SchemaVersion:         SchemaVersion,
		DesiredArtifactDigest: subject.Digest.String(),
		CurrentSchemaDigest:   digest.FromBytes(currentJSON).String(),
		Dialect:               dialect,
		Schemas:               schemas,
		Capabilities:          capabilities,
		Assessments:           assessments,
	}, nil
}

// NewFS returns an immutable filesystem containing the canonical plan report.
func NewFS(report Report) (fs.FS, error) {
	if report.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported migration plan schema version %d", report.SchemaVersion)
	}
	contents, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("render migration plan: %w", err)
	}
	contents = append(contents, '\n')
	snapshot, err := fsnapshot.FromFiles(map[string][]byte{FileName: contents})
	if err != nil {
		return nil, fmt.Errorf("build migration plan filesystem: %w", err)
	}
	return snapshot, nil
}

// Publish attaches assessments to an already resolved schema artifact.
func Publish(
	ctx context.Context,
	client *ociartifact.Client,
	subjectRef string,
	subject ocispec.Descriptor,
	report Report,
) (ociartifact.PushResult, error) {
	if client == nil {
		return ociartifact.PushResult{}, fmt.Errorf("OCI client is required")
	}
	if report.DesiredArtifactDigest != subject.Digest.String() {
		return ociartifact.PushResult{}, fmt.Errorf("migration plan desired artifact digest does not match attachment subject")
	}
	planFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return client.AttachResolved(ctx, subjectRef, subject, planFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.PlanArtifactType,
		LayerMediaType: LayerMediaType,
	})
}

// PublishTo attaches assessments to subject in target.
func PublishTo(
	ctx context.Context,
	target oras.Target,
	subject ocispec.Descriptor,
	report Report,
) (ociartifact.PushResult, error) {
	if report.DesiredArtifactDigest != subject.Digest.String() {
		return ociartifact.PushResult{}, fmt.Errorf("migration plan desired artifact digest does not match attachment subject")
	}
	planFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return ociartifact.AttachTo(ctx, target, subject, planFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.PlanArtifactType,
		LayerMediaType: LayerMediaType,
	})
}
