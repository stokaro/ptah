package ociartifact_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"

	"go.5x5.cz/ptah/internal/ociartifact"
)

const evidenceTestArtifactType = "application/vnd.ptah.evidence.v1+json"

// TestAttachRoundTripsInAnImageLayout is stokaro/ptah#2839, the half of
// stokaro/ptah#2623 that did not come along.
//
// `Push` and `Pull` needed one thing each from a reference and `Attach` needed
// more, so an air-gapped producer could publish a release to a directory and
// could not attach a verification to it. Measured before this, the same call
// answered `invalid OCI reference: expected oci:// prefix`.
//
// The round trip is one test because either half proves less alone: an
// attachment nothing lists back has not been shown to be discoverable, which
// is the entire point of the operation.
//
// It is also load-bearing in a way worth naming, because `Referrers` opens its
// own store on the same directory. A layout reopened that way answers
// `Predecessors` with the artifact type EMPTY -- measured -- so a listing that
// took the type from the predecessor descriptor filtered every referrer away
// on the first call after a restart, while passing any test that listed
// through the same store that had just written. Asking for the type here is
// what catches that.
func TestAttachRoundTripsInAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v1"

	subject, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{"generation":"g1"}`)}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)

	attached, err := ociartifact.Attach(ctx, reference,
		fstest.MapFS{"evidence.json": &fstest.MapFile{Data: []byte(`{"verified":true}`)}},
		ociartifact.AttachmentOptions{ArtifactType: evidenceTestArtifactType})
	c.Assert(err, qt.IsNil)
	c.Assert(attached.Descriptor.Digest, qt.Not(qt.Equals), subject.Descriptor.Digest)

	referrers, err := ociartifact.Referrers(ctx, reference, evidenceTestArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(referrers, qt.HasLen, 1)
	c.Assert(referrers[0].Digest, qt.Equals, attached.Descriptor.Digest)
	c.Assert(referrers[0].ArtifactType, qt.Equals, evidenceTestArtifactType)
}

// TestLayoutReferrersExcludeAManifestThatOnlyCarriesTheSubject is the control
// the adapter exists for.
//
// A layout has no /referrers endpoint, so the listing is built from the store's
// reverse graph -- and that graph answers every manifest POINTING at the
// subject, which is a superset. The second manifest below carries the subject
// as a LAYER and declares no `subject` of its own, so the store reports it as a
// predecessor and a registry's /referrers could never return it.
//
// The first version of this test put the unrelated artifact in a different
// directory, where it was not a predecessor of anything. It passed, and it
// proved nothing: removing the subject filter entirely left it green. This
// version builds the predecessor in the same layout, which is the only way the
// filter is measured.
func TestLayoutReferrersExcludeAManifestThatOnlyCarriesTheSubject(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	directory := c.TempDir() + "/store"
	reference := ociartifact.LayoutScheme + directory + ":v1"

	_, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)
	attached, err := ociartifact.Attach(ctx, reference,
		fstest.MapFS{"evidence.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.AttachmentOptions{ArtifactType: evidenceTestArtifactType})
	c.Assert(err, qt.IsNil)

	store, err := oci.New(directory)
	c.Assert(err, qt.IsNil)
	subject, err := store.Resolve(ctx, "v1")
	c.Assert(err, qt.IsNil)
	carrier, err := oras.PackManifest(ctx, store, oras.PackManifestVersion1_1,
		"application/vnd.ptah.carrier", oras.PackManifestOptions{
			Layers: []ocispec.Descriptor{subject},
		})
	c.Assert(err, qt.IsNil)
	predecessors, err := store.Predecessors(ctx, subject)
	c.Assert(err, qt.IsNil)
	c.Assert(predecessors, qt.HasLen, 2,
		qt.Commentf("the store must report both, or this control measures nothing"))

	all, err := ociartifact.Referrers(ctx, reference, "")

	c.Assert(err, qt.IsNil)
	c.Assert(all, qt.HasLen, 1)
	c.Assert(all[0].Digest, qt.Equals, attached.Descriptor.Digest)
	c.Assert(all[0].Digest, qt.Not(qt.Equals), carrier.Digest)
}

// TestAttachToALayoutSatisfiesRequiredAPI records the policy decision, which
// is the part stokaro/ptah#2839 said not to guess at.
//
// `required-api` exists to stop a publish whose attachment nothing could
// discover. A directory has no referrers endpoint, and discovery there is the
// store's own graph, which works. Refusing would refuse a publish that IS
// discoverable, for the sake of a mechanism that is not how discovery happens
// in a layout.
//
// TestResolveAttachmentPolicy_RequiredAPIRefusesBeforeWriting is the other
// half and is unchanged: a REGISTRY that does not serve the index still fails
// this policy, before anything is written. Without that pairing this test
// would read as "we stopped checking".
func TestAttachToALayoutSatisfiesRequiredAPI(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v1"

	_, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)

	attached, err := ociartifact.Attach(ctx, reference,
		fstest.MapFS{"evidence.json": &fstest.MapFile{Data: []byte(`{}`)}},
		ociartifact.AttachmentOptions{
			ArtifactType: evidenceTestArtifactType,
			Policy:       ociartifact.ReferrerPolicyRequiredAPI,
		})

	c.Assert(err, qt.IsNil)
	c.Assert(attached.Descriptor.Digest.String(), qt.Not(qt.Equals), "")
}
