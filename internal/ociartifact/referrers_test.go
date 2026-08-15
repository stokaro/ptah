package ociartifact_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func TestAttachTo_CreatesSubjectManifest(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subjectResult, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)

	attachment, err := ociartifact.AttachTo(ctx, store, subjectResult.Descriptor, fstest.MapFS{
		"lint.json": {Data: []byte(`{"findings":[]}`)},
	}, ociartifact.AttachmentOptions{
		ArtifactType: ociartifact.LintArtifactType,
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	manifestBytes, err := content.FetchAll(ctx, store, attachment.Descriptor)
	c.Assert(err, qt.IsNil)
	var manifest ocispec.Manifest
	err = json.Unmarshal(manifestBytes, &manifest)
	c.Assert(err, qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.LintArtifactType)
	c.Assert(manifest.Subject, qt.IsNotNil)
	c.Assert(manifest.Subject.Digest, qt.Equals, subjectResult.Descriptor.Digest)
	c.Assert(attachment.Tags, qt.HasLen, 1)
	c.Assert(attachment.Tags[0], qt.Matches, `ptah-r-[a-f0-9]{32}-[a-f0-9]{64}`)
	tagged, err := store.Resolve(ctx, attachment.Tags[0])
	c.Assert(err, qt.IsNil)
	c.Assert(tagged.Digest, qt.Equals, attachment.Descriptor.Digest)

	err = store.Tag(ctx, attachment.Descriptor, "attachment")
	c.Assert(err, qt.IsNil)
	pulled, err := ociartifact.PullFrom(
		ctx,
		store,
		"attachment",
		ociartifact.PullOptions{ExpectedArtifactTypes: []string{ociartifact.LintArtifactType}},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(pulled.FileSystem, "lint.json"), qt.IsNil)
}

func TestAttachTo_FailurePath(t *testing.T) {

	t.Run("missing subject", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.AttachTo(
			context.Background(),
			memory.New(),
			ocispec.Descriptor{},
			fstest.MapFS{"lint.json": {Data: []byte("{}")}},
			ociartifact.AttachmentOptions{ArtifactType: ociartifact.LintArtifactType},
		)
		c.Assert(err, qt.ErrorMatches, "attachment subject digest is required")
	})
}

func TestAttachTo_PreservesPartialStateWhenDurableTagFails(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
	})
	c.Assert(err, qt.IsNil)
	target := &rejectingTagTarget{Target: store}

	result, err := ociartifact.AttachTo(ctx, target, subject.Descriptor, fstest.MapFS{
		"lint.json": {Data: []byte(`{"findings":[]}`)},
	}, ociartifact.AttachmentOptions{
		ArtifactType: ociartifact.LintArtifactType,
	})

	var partial *ociartifact.PartialPushError
	c.Assert(err, qt.ErrorAs, &partial)
	c.Assert(partial.Err, qt.ErrorIs, errInjectedTag)
	c.Assert(result.Descriptor.Digest, qt.Equals, partial.Descriptor.Digest)
	c.Assert(partial.FailedTag, qt.Matches, `ptah-r-[a-f0-9]{32}-[a-f0-9]{64}`)
}

func TestListReferrersFrom_CollectsPagesAndClonesAnnotations(t *testing.T) {
	c := qt.New(t)
	subject := ocispec.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	firstAnnotations := map[string]string{"kind": "lint"}
	lister := &referrerLister{
		pages: [][]ocispec.Descriptor{
			{{Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Annotations: firstAnnotations}},
			{{Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
		},
	}

	got, err := ociartifact.ListReferrersFrom(context.Background(), lister, subject, ociartifact.LintArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 2)
	c.Assert(lister.artifactType, qt.Equals, ociartifact.LintArtifactType)
	c.Assert(lister.subject.Digest, qt.Equals, subject.Digest)
	firstAnnotations["kind"] = "mutated"
	c.Assert(got[0].Annotations["kind"], qt.Equals, "lint")
}

func TestListReferrersFrom_FailurePath(t *testing.T) {

	t.Run("missing lister", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.ListReferrersFrom(
			context.Background(),
			nil,
			ocispec.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			"",
		)
		c.Assert(err, qt.ErrorMatches, "OCI referrer lister is required")
	})

	t.Run("missing subject", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.ListReferrersFrom(
			context.Background(),
			&referrerLister{},
			ocispec.Descriptor{},
			"",
		)
		c.Assert(err, qt.ErrorMatches, "referrer subject digest is required")
	})

	t.Run("lister error", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.ListReferrersFrom(
			context.Background(),
			&referrerLister{err: context.DeadlineExceeded},
			ocispec.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			"",
		)
		c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	})

	t.Run("referrer limit", func(t *testing.T) {
		c := qt.New(t)
		_, err := ociartifact.ListReferrersFrom(
			context.Background(),
			&referrerLister{pages: [][]ocispec.Descriptor{make([]ocispec.Descriptor, 1001)}},
			ocispec.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			"",
		)
		c.Assert(err, qt.ErrorIs, ociartifact.ErrArtifactLimit)
	})
}

type referrerLister struct {
	pages        [][]ocispec.Descriptor
	err          error
	subject      ocispec.Descriptor
	artifactType string
}

var errInjectedTag = errors.New("injected tag failure")

type rejectingTagTarget struct {
	oras.Target
}

func (t *rejectingTagTarget) Tag(
	context.Context,
	ocispec.Descriptor,
	string,
) error {
	return errInjectedTag
}

func (l *referrerLister) Referrers(
	_ context.Context,
	subject ocispec.Descriptor,
	artifactType string,
	fn func([]ocispec.Descriptor) error,
) error {
	l.subject = subject
	l.artifactType = artifactType
	for _, page := range l.pages {
		if err := fn(page); err != nil {
			return err
		}
	}
	return l.err
}
