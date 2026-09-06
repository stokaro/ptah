package embedrelease

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"testing/fstest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/internal/ociartifact"
)

// FileName is what each record is called inside its artifact.
//
// One name per kind rather than one shared name, so a person who has fetched
// three artifacts into one directory can still tell them apart -- which is what
// happens the moment somebody investigates a cutover.
const (
	ReleaseFileName      = "release.json"
	VerificationFileName = "verification.json"
	CutoverFileName      = "cutover.json"
	RollbackFileName     = "rollback.json"
	RetirementFileName   = "retirement.json"
	// SpecificationFileName is the document a release was built from, carried
	// beside the record rather than described by it.
	//
	// A release exists to be promoted, and an environment promoting one has
	// never seen the operator's file. Carrying only its digest would name a
	// document that environment cannot produce, so the file would have to
	// travel by some other path -- and a specification that arrives beside its
	// release is one nothing checked against it.
	SpecificationFileName = "specification.yaml"
)

// Record is one piece of evidence, ready to publish.
type Record struct {
	// ArtifactType is the OCI artifact type.
	ArtifactType string
	// FileName is what the layer is called.
	FileName string
	// Body is the encoded record.
	Body []byte
	// Digest is the record's own content address, which is not the manifest's.
	//
	// Both are worth having: the manifest digest identifies what a registry
	// holds, and this identifies what the record SAYS. A record republished to
	// two registries has two manifest digests and one of these.
	Digest string
	// Annotations are what a registry lists without pulling the layer.
	Annotations map[string]string
	// Files are what travels beside the record, by name.
	//
	// Only a release has any: the document it was built from. A verification
	// and a cutover describe something that already exists and carry nothing an
	// environment would need to reproduce.
	Files map[string][]byte
}

// NewReleaseRecord prepares a release for publication.
//
// The specification is a parameter rather than a field a caller may leave
// empty, because a release without it is one that cannot be promoted -- and a
// promotion that fails on arrival, in another environment, is a long way from
// the run that published it.
func NewReleaseRecord(release Release, specification []byte) (Record, error) {
	release.Version = RecordVersion
	if len(specification) == 0 {
		return Record{}, fmt.Errorf(
			"a release carries the specification it was built from, and generation %s was given none",
			release.Generation)
	}
	body, err := Encode(release)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ArtifactType: ReleaseArtifactType, FileName: ReleaseFileName,
		Body: body, Digest: release.Digest(),
		Annotations: map[string]string{
			"run.ptah.inference.generation":      release.Generation,
			"run.ptah.inference.record":          release.Digest(),
			"run.ptah.inference.reproducibility": release.Reproducibility,
			// The specification's own address, listed so that a reader
			// comparing two releases can see whether the document changed
			// without pulling either layer.
			"run.ptah.inference.specification": release.SpecDigest,
		},
		Files: map[string][]byte{SpecificationFileName: specification},
	}, nil
}

// NewVerificationRecord prepares a verification report for publication.
func NewVerificationRecord(verification Verification) (Record, error) {
	verification.Version = RecordVersion
	body, err := Encode(verification)
	if err != nil {
		return Record{}, err
	}
	// The verdict is an annotation because a registry lists annotations without
	// pulling the layer, and "did this pass" is the question somebody scanning
	// a list of reports is asking.
	annotations := map[string]string{
		"run.ptah.inference.generation": verification.Generation,
		"run.ptah.inference.record":     verification.Digest(),
		"run.ptah.inference.passed":     "false",
	}
	if verification.Passed {
		annotations["run.ptah.inference.passed"] = "true"
	}
	return Record{
		ArtifactType: VerificationArtifactType, FileName: VerificationFileName,
		Body: body, Digest: verification.Digest(), Annotations: annotations,
	}, nil
}

// NewCutoverRecord prepares a cutover record for publication.
func NewCutoverRecord(cutover Cutover) (Record, error) {
	cutover.Version = RecordVersion
	body, err := Encode(cutover)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ArtifactType: CutoverArtifactType, FileName: CutoverFileName,
		Body: body, Digest: cutover.Digest(),
		Annotations: map[string]string{
			"run.ptah.inference.generation": cutover.Generation,
			"run.ptah.inference.record":     cutover.Digest(),
			"run.ptah.inference.plan":       cutover.PlanDigest,
		},
	}, nil
}

// NewRollbackRecord prepares a rollback record for publication.
func NewRollbackRecord(rollback Rollback) (Record, error) {
	rollback.Version = RecordVersion
	body, err := Encode(rollback)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ArtifactType: RollbackArtifactType, FileName: RollbackFileName,
		Body: body, Digest: rollback.Digest(),
		Annotations: map[string]string{
			"run.ptah.inference.generation": rollback.Generation,
			"run.ptah.inference.record":     rollback.Digest(),
			"run.ptah.inference.replaced":   rollback.Replaced,
		},
	}, nil
}

// NewRetirementRecord prepares a retirement record for publication.
//
// The annotations name the generation and how much went with it, because this
// is the one record whose subject a reader cannot go and look at.
func NewRetirementRecord(retirement Retirement) (Record, error) {
	retirement.Version = RecordVersion
	body, err := Encode(retirement)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ArtifactType: RetirementArtifactType, FileName: RetirementFileName,
		Body: body, Digest: retirement.Digest(),
		Annotations: map[string]string{
			"run.ptah.inference.generation": retirement.Generation,
			"run.ptah.inference.record":     retirement.Digest(),
			"run.ptah.inference.rows":       strconv.FormatInt(retirement.Rows, 10),
		},
	}, nil
}

// Publish pushes one record to a registry.
//
// The record is its own artifact rather than a layer appended to something
// else, because evidence accumulates: a generation gets one release, several
// verifications and possibly two cutovers, and a single mutable artifact
// holding all of them would have to be rewritten -- which is the one thing
// evidence must not be.
func Publish(
	ctx context.Context, reference string, record Record, opts PublishOptions,
) (ociartifact.PushResult, error) {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return ociartifact.PushResult{}, fmt.Errorf("publish %s: %w", record.ArtifactType, err)
	}
	result, err := client.Push(ctx, reference, recordFS(record), ociartifact.PushOptions{
		ArtifactType: record.ArtifactType,
		Annotations:  recordAnnotations(record, opts),
		Subject:      opts.Subject,
		Tags:         opts.Tags,
	})
	if err != nil {
		return ociartifact.PushResult{}, fmt.Errorf("publish %s: %w", record.ArtifactType, err)
	}
	return result, nil
}

// Attach publishes one record as a referrer of the artifact subjectRef names.
//
// It is a second function rather than a field on [Publish] because the two have
// different destinations and only one of them is the caller's to choose: a
// referrer lands in its subject's repository, so a reference saying where to
// push it would be a second answer that the registry ignores.
//
// This is how a verification is found from the release it is about. A
// generation gets one release and several verifications, and finding them by
// remembering a tag for each is how a record goes missing.
func Attach(
	ctx context.Context, subjectRef string, record Record, opts PublishOptions,
) (ociartifact.PushResult, error) {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return ociartifact.PushResult{}, fmt.Errorf("attach %s: %w", record.ArtifactType, err)
	}
	result, err := client.Attach(ctx, subjectRef, recordFS(record), ociartifact.AttachmentOptions{
		ArtifactType: record.ArtifactType,
		Annotations:  recordAnnotations(record, opts),
	})
	if err != nil {
		return ociartifact.PushResult{}, fmt.Errorf("attach %s: %w", record.ArtifactType, err)
	}
	return result, nil
}

// recordFS is the archive a record travels as: the record itself, and whatever
// it carries beside it.
func recordFS(record Record) fstest.MapFS {
	archive := fstest.MapFS{record.FileName: &fstest.MapFile{Data: record.Body, Mode: 0o600}}
	for name, body := range record.Files {
		archive[name] = &fstest.MapFile{Data: body, Mode: 0o600}
	}
	return archive
}

// recordAnnotations merges the record's own annotations with the caller's and
// stamps when it was written down.
func recordAnnotations(record Record, opts PublishOptions) map[string]string {
	annotations := make(map[string]string, len(record.Annotations)+len(opts.Annotations))
	maps.Copy(annotations, record.Annotations)
	maps.Copy(annotations, opts.Annotations)
	if !opts.RecordedAt.IsZero() {
		annotations["org.opencontainers.image.created"] = opts.RecordedAt.UTC().Format(time.RFC3339)
	}
	return annotations
}

// PublishOptions are what a caller adds to a record.
type PublishOptions struct {
	// Subject attaches the record to another artifact as a referrer, which is
	// how a verification is found from the release it is about rather than by
	// remembering a tag.
	Subject *ocispec.Descriptor
	// Tags are the mutable names to point at it, and are usually none: a piece
	// of evidence is addressed by digest.
	Tags []string
	// Annotations are added to the record's own.
	Annotations map[string]string
	// RecordedAt stamps the artifact, and is separate from the record's own
	// timestamp: one is when the thing happened, the other is when somebody
	// wrote it down, and a replayed publication has two different answers.
	RecordedAt time.Time
	// PlainHTTP permits an unencrypted connection to the registry. Evidence is
	// not a secret -- it is written to be read -- but the credential presented
	// to push it is, so this stays off unless somebody asks for it.
	PlainHTTP bool
}
