package embedrelease_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/ociartifact"
)

// TestAttach_WritesAReferrerIntoAnImageLayout is stokaro/ptah#2839, the half of
// stokaro/ptah#2623 that did not come along.
//
// [TestPublish_WritesARunnableReleaseIntoAnImageLayout] covers
// `--publish-evidence`, which puts a record somewhere of its own.
// `--attach-to` is the other flag and the other operation: it makes the record
// a referrer of a release, which is how it is found without remembering a tag.
// Measured before this, against a layout it answered
// `invalid OCI reference: expected oci:// prefix`.
//
// The assertion lists the attachment back rather than stopping at the push,
// because an attachment nothing can discover is the one thing this operation
// exists to avoid.
func TestAttach_WritesAReferrerIntoAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := layoutIn(c) + ":v3"
	release, err := embedrelease.NewReleaseRecord(aRelease(), []byte(specification))
	c.Assert(err, qt.IsNil)
	subject, err := embedrelease.Publish(ctx, reference, release, embedrelease.PublishOptions{})
	c.Assert(err, qt.IsNil)

	verification, err := embedrelease.NewVerificationRecord(aVerification())
	c.Assert(err, qt.IsNil)
	attached, err := embedrelease.Attach(ctx, reference, verification, embedrelease.PublishOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(attached.Descriptor.Digest, qt.Not(qt.Equals), subject.Descriptor.Digest)

	referrers, err := ociartifact.Referrers(ctx, reference, verification.ArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(referrers, qt.HasLen, 1)
	c.Assert(referrers[0].Digest, qt.Equals, attached.Descriptor.Digest)
	c.Assert(referrers[0].ArtifactType, qt.Equals, verification.ArtifactType)
}
