package ociartifact_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ociartifact"
)

// TestReferrersListsWhatWasAttachedToALayout is stokaro/ptah#2852.
//
// stokaro/ptah#2839 made an attachment writable into a directory and left the
// listing on the registry path, so measured on the commit that landed it, an
// attach succeeded and `Referrers` over the same layout answered
// `invalid OCI reference: expected oci:// prefix`. The operation was still
// sound -- discoverability is checked inside the write -- but the only caller
// who could ask what was attached was the call that attached it, and the
// air-gapped consumer the layout exists for could list nothing.
//
// The listing is a SEPARATE call on purpose, and that is what makes the
// assertion real: it opens its own store over the directory, and a store
// reopened that way hands back predecessors with no artifact type. Asserting
// the artifact type rather than only the digest is what would catch a listing
// that trusted the predecessor descriptor.
func TestReferrersListsWhatWasAttachedToALayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v1"

	_, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)
	attached, err := ociartifact.Attach(ctx, reference,
		fstest.MapFS{"evidence.json": &fstest.MapFile{Data: []byte(`{"verified":true}`)}},
		ociartifact.AttachmentOptions{ArtifactType: layoutReadBackArtifactType})
	c.Assert(err, qt.IsNil)

	listed, err := ociartifact.Referrers(ctx, reference, layoutReadBackArtifactType)

	c.Assert(err, qt.IsNil)
	c.Assert(listed, qt.HasLen, 1)
	c.Assert(listed[0].Digest, qt.Equals, attached.Descriptor.Digest)
	c.Assert(listed[0].ArtifactType, qt.Equals, layoutReadBackArtifactType,
		qt.Commentf("a reopened store reports no artifact type, so this is the half that matters"))
}

// TestReferrersFiltersByArtifactTypeInALayout keeps the filter honest: a
// listing that ignored the type would satisfy the test above by returning
// everything.
func TestReferrersFiltersByArtifactTypeInALayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v1"

	_, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)
	_, err = ociartifact.Attach(ctx, reference,
		fstest.MapFS{"evidence.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.AttachmentOptions{ArtifactType: layoutReadBackArtifactType})
	c.Assert(err, qt.IsNil)

	listed, err := ociartifact.Referrers(ctx, reference, "application/vnd.ptah.absent.v1+json")

	c.Assert(err, qt.IsNil)
	c.Assert(listed, qt.HasLen, 0)
}

// TestReferrersStillRefusesSomethingThatIsNoReference is the control that
// keeps this from reading as "we stopped parsing references". A registry
// reference and a malformed one answer exactly as they did.
func TestReferrersStillRefusesSomethingThatIsNoReference(t *testing.T) {
	c := qt.New(t)

	_, err := ociartifact.Referrers(context.Background(), "not a reference at all", "")

	c.Assert(err, qt.IsNotNil)
}

const layoutReadBackArtifactType = "application/vnd.ptah.readback.v1+json"
