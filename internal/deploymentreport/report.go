// Package deploymentreport builds redacted, deterministic deployment reports.
package deploymentreport

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"ptah.run/internal/fsnapshot"
	"ptah.run/internal/ociartifact"
	"ptah.run/migration/migrator"
)

const (
	// FileName is the artifact layer name used for deployment reports.
	FileName = "deployment.json"

	// LayerMediaType identifies a deployment report JSON layer.
	LayerMediaType = ociartifact.DeploymentReportLayerMediaType

	// SchemaVersion identifies the deployment report JSON contract.
	SchemaVersion = 1
)

// Outcome identifies the result of a migration deployment.
type Outcome string

const (
	// OutcomeSucceeded records a successfully completed migration deployment.
	OutcomeSucceeded Outcome = "succeeded"
)

// ErrInvalidReport identifies a deployment report validation failure.
var ErrInvalidReport = errors.New("invalid deployment report")

var (
	deploymentIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	digestPattern       = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	dialectPattern      = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
)

// Report contains the non-sensitive facts recorded after a migration run.
//
// Report deliberately has no fields for database URLs, hostnames, environment
// values, local paths, SQL text, or credentials.
type Report struct {
	DeploymentID            string
	MigrationArtifactDigest string
	Dialect                 string
	FromVersion             int64
	ToVersion               int64
	AppliedVersions         []int64
	// KeyedByPosition reports that the migrations ran under their positions in
	// file-name order (see [migrator.Migrator.KeyedByPosition]). A position
	// moves when a file is added before it, so such a report names each
	// migration by its version key: FromVersionKey, ToVersionKey and
	// AppliedVersionKeys are written in place of the three numeric fields,
	// which are neither validated nor written. An empty FromVersionKey names
	// no migration.
	KeyedByPosition    bool
	FromVersionKey     string
	ToVersionKey       string
	AppliedVersionKeys []string
	StartedAt          time.Time
	FinishedAt         time.Time
	Outcome            Outcome
}

// SuccessfulOptions contains the committed migration state required to record
// a successful deployment against an exact OCI migration artifact.
type SuccessfulOptions struct {
	Subject    ocispec.Descriptor
	Dialect    string
	Before     *migrator.MigrationStatus
	After      *migrator.MigrationStatus
	StartedAt  time.Time
	FinishedAt time.Time
	// KeyedByPosition is the migrator's [migrator.Migrator.KeyedByPosition]:
	// the report then names the migrations by the version keys of Before and
	// After.
	KeyedByPosition bool
}

type wireReport struct {
	SchemaVersion           int       `json:"schema_version"`
	DeploymentID            string    `json:"deployment_id"`
	MigrationArtifactDigest string    `json:"migration_artifact_digest"`
	Dialect                 string    `json:"dialect"`
	FromVersion             int64     `json:"from_version"`
	ToVersion               int64     `json:"to_version"`
	AppliedVersions         []int64   `json:"applied_versions"`
	StartedAt               time.Time `json:"started_at"`
	FinishedAt              time.Time `json:"finished_at"`
	Outcome                 Outcome   `json:"outcome"`
}

// keyedWireReport is wireReport with the version keys in place of the
// numbers, for a report whose numbers are positions.
type keyedWireReport struct {
	SchemaVersion           int       `json:"schema_version"`
	DeploymentID            string    `json:"deployment_id"`
	MigrationArtifactDigest string    `json:"migration_artifact_digest"`
	Dialect                 string    `json:"dialect"`
	FromVersionKey          string    `json:"from_version_key"`
	ToVersionKey            string    `json:"to_version_key"`
	AppliedVersionKeys      []string  `json:"applied_version_keys"`
	StartedAt               time.Time `json:"started_at"`
	FinishedAt              time.Time `json:"finished_at"`
	Outcome                 Outcome   `json:"outcome"`
}

// NewSuccessfulReport builds and validates a redacted report from committed
// migration state before and after a successful migration run.
func NewSuccessfulReport(opts SuccessfulOptions) (Report, error) {
	if opts.Before == nil {
		return Report{}, invalidField("before migration status is required")
	}
	if opts.After == nil {
		return Report{}, invalidField("after migration status is required")
	}

	report := Report{
		DeploymentID:            "deployment-" + rand.Text(),
		MigrationArtifactDigest: opts.Subject.Digest.String(),
		Dialect:                 opts.Dialect,
		FromVersion:             opts.Before.CurrentVersion,
		ToVersion:               opts.After.CurrentVersion,
		AppliedVersions: AppliedVersionDelta(
			opts.Before.AppliedMigrations,
			opts.After.AppliedMigrations,
		),
		StartedAt:  opts.StartedAt,
		FinishedAt: opts.FinishedAt,
		Outcome:    OutcomeSucceeded,
	}
	if opts.KeyedByPosition {
		report.KeyedByPosition = true
		report.FromVersionKey = opts.Before.CurrentVersionKey
		report.ToVersionKey = opts.After.CurrentVersionKey
		report.AppliedVersionKeys = appliedKeyDelta(opts.Before.AppliedMigrationKeys, opts.After.AppliedMigrationKeys)
	}
	if err := validate(report); err != nil {
		return Report{}, err
	}
	return report, nil
}

// AppliedVersionDelta returns the sorted versions present after a deployment
// but absent before it. It derives report data from committed revision state
// instead of assuming every migration that was pending was executed.
func AppliedVersionDelta(before, after []int64) []int64 {
	previous := make(map[int64]struct{}, len(before))
	for _, version := range before {
		previous[version] = struct{}{}
	}

	seen := make(map[int64]struct{}, len(after))
	applied := make([]int64, 0, len(after))
	for _, version := range after {
		if _, existed := previous[version]; existed {
			continue
		}
		if _, duplicate := seen[version]; duplicate {
			continue
		}
		seen[version] = struct{}{}
		applied = append(applied, version)
	}
	slices.Sort(applied)
	return applied
}

// appliedKeyDelta returns the version keys present after a deployment but
// absent before it, in the order after lists them, which is the order the
// migrations run in.
func appliedKeyDelta(before, after []string) []string {
	applied := make([]string, 0, len(after))
	for _, key := range after {
		if !slices.Contains(before, key) && !slices.Contains(applied, key) {
			applied = append(applied, key)
		}
	}
	return applied
}

// HasAppliedChanges reports whether committed revision state gained at least
// one migration version during a run.
func HasAppliedChanges(before, after *migrator.MigrationStatus) bool {
	return before != nil &&
		after != nil &&
		len(AppliedVersionDelta(before.AppliedMigrations, after.AppliedMigrations)) > 0
}

// NewFS validates report and returns an immutable filesystem containing its
// canonical JSON representation as deployment.json.
func NewFS(report Report) (fs.FS, error) {
	if err := validate(report); err != nil {
		return nil, err
	}

	contents, err := marshal(report)
	if err != nil {
		return nil, err
	}

	snapshot, err := fsnapshot.FromFiles(map[string][]byte{FileName: contents})
	if err != nil {
		return nil, fmt.Errorf("build deployment report filesystem: %w", err)
	}
	return snapshot, nil
}

// PublishSuccessful builds and attaches a successful deployment report to the
// exact OCI migration artifact described by opts.Subject.
func PublishSuccessful(
	ctx context.Context,
	client *ociartifact.Client,
	subjectRef string,
	opts SuccessfulOptions,
) (ociartifact.PushResult, error) {
	report, err := NewSuccessfulReport(opts)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return Publish(ctx, client, subjectRef, opts.Subject, report)
}

// Publish attaches report to an already resolved migration artifact.
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
	if err := validateSubject(report, subject); err != nil {
		return ociartifact.PushResult{}, err
	}
	reportFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return client.AttachResolved(ctx, subjectRef, subject, reportFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.DeploymentArtifactType,
		LayerMediaType: LayerMediaType,
	})
}

// PublishTo attaches report to subject in target.
func PublishTo(
	ctx context.Context,
	target oras.Target,
	subject ocispec.Descriptor,
	report Report,
) (ociartifact.PushResult, error) {
	if err := validateSubject(report, subject); err != nil {
		return ociartifact.PushResult{}, err
	}
	reportFS, err := NewFS(report)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return ociartifact.AttachTo(ctx, target, subject, reportFS, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.DeploymentArtifactType,
		LayerMediaType: LayerMediaType,
	})
}

func validateSubject(report Report, subject ocispec.Descriptor) error {
	if report.MigrationArtifactDigest != subject.Digest.String() {
		return invalidField("migration artifact digest does not match attachment subject")
	}
	return nil
}

func validate(report Report) error {
	if report.DeploymentID == "" {
		return invalidField("deployment ID is required")
	}
	if !deploymentIDPattern.MatchString(report.DeploymentID) {
		return invalidField("deployment ID has an invalid format")
	}
	if report.MigrationArtifactDigest == "" {
		return invalidField("migration artifact digest is required")
	}
	if !digestPattern.MatchString(report.MigrationArtifactDigest) {
		return invalidField("migration artifact digest has an invalid format")
	}
	if report.Dialect == "" {
		return invalidField("dialect is required")
	}
	if !dialectPattern.MatchString(report.Dialect) {
		return invalidField("dialect has an invalid format")
	}
	if err := validateVersions(report); err != nil {
		return err
	}
	if !validTimestamp(report.StartedAt) {
		return invalidField("started timestamp is invalid")
	}
	if !validTimestamp(report.FinishedAt) {
		return invalidField("finished timestamp is invalid")
	}
	if report.FinishedAt.Before(report.StartedAt) {
		return invalidField("finished timestamp must not be before started timestamp")
	}
	if report.Outcome != OutcomeSucceeded {
		return invalidField("outcome is invalid")
	}
	return nil
}

func validateVersions(report Report) error {
	if report.KeyedByPosition {
		return validateVersionKeys(report)
	}
	if report.FromVersion < 0 {
		return invalidField("from version must not be negative")
	}
	if report.ToVersion < report.FromVersion {
		return invalidField("to version must not be less than from version")
	}
	return validateAppliedVersions(report.AppliedVersions)
}

// validateVersionKeys holds a keyed report to what a run that applied
// something leaves: a key it ended at, and applied keys that are non-empty
// and distinct. Keys have no order to compare, since a key that is not a
// number sorts nowhere among the numbers.
func validateVersionKeys(report Report) error {
	if report.ToVersionKey == "" {
		return invalidField("to version key is required")
	}
	for index, key := range report.AppliedVersionKeys {
		if key == "" {
			return invalidField("applied version keys must not be empty")
		}
		if slices.Contains(report.AppliedVersionKeys[:index], key) {
			return invalidField("applied version keys must be distinct")
		}
	}
	return nil
}

func validateAppliedVersions(versions []int64) error {
	var previous int64
	for index, version := range versions {
		if version <= 0 {
			return invalidField("applied versions must be positive")
		}
		if index > 0 && version <= previous {
			return invalidField("applied versions must be strictly increasing")
		}
		previous = version
	}
	return nil
}

func validTimestamp(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func invalidField(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidReport, reason)
}

func marshal(report Report) ([]byte, error) {
	var canonical any = wireReport{
		SchemaVersion:           SchemaVersion,
		DeploymentID:            report.DeploymentID,
		MigrationArtifactDigest: report.MigrationArtifactDigest,
		Dialect:                 report.Dialect,
		FromVersion:             report.FromVersion,
		ToVersion:               report.ToVersion,
		AppliedVersions:         append(make([]int64, 0), report.AppliedVersions...),
		StartedAt:               report.StartedAt.Round(0).UTC(),
		FinishedAt:              report.FinishedAt.Round(0).UTC(),
		Outcome:                 report.Outcome,
	}
	if report.KeyedByPosition {
		canonical = keyedWireReport{
			SchemaVersion:           SchemaVersion,
			DeploymentID:            report.DeploymentID,
			MigrationArtifactDigest: report.MigrationArtifactDigest,
			Dialect:                 report.Dialect,
			FromVersionKey:          report.FromVersionKey,
			ToVersionKey:            report.ToVersionKey,
			AppliedVersionKeys:      append(make([]string, 0), report.AppliedVersionKeys...),
			StartedAt:               report.StartedAt.Round(0).UTC(),
			FinishedAt:              report.FinishedAt.Round(0).UTC(),
			Outcome:                 report.Outcome,
		}
	}
	contents, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal deployment report: %w", err)
	}
	return append(contents, '\n'), nil
}
