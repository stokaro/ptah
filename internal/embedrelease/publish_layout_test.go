package embedrelease_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrelease"
)

// TestPublish_WritesARunnableReleaseIntoAnImageLayout is stokaro/ptah#2623: the
// producing half of an air-gapped promotion.
//
// [TestFetch_ReadsBackWhatWasPublished] already reads a release out of a
// layout, and it writes one by assembling the archive itself. That proves the
// reader, and it is exactly why the gap survived: nothing asked whether
// [Publish] -- the function the CLI actually calls -- could write one. It could
// not, answering `invalid OCI reference: expected oci:// prefix`, so an
// environment with no registry could consume a promotion and never produce one.
//
// What makes this a release rather than a record is the specification travelling
// with it, which is the distinction `--evidence-file` does not cross: it writes
// `release.json` alone, and a release is runnable elsewhere because it carries
// the document too. So the assertion reads the specification back rather than
// stopping at the generation.
func TestPublish_WritesARunnableReleaseIntoAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := layoutIn(c) + ":v3"
	record, err := embedrelease.NewReleaseRecord(aRelease(), []byte(specification))
	c.Assert(err, qt.IsNil)

	published, err := embedrelease.Publish(ctx, reference, record, embedrelease.PublishOptions{})
	c.Assert(err, qt.IsNil)

	fetched, err := embedrelease.Fetch(ctx, reference, embedrelease.FetchOptions{})
	c.Assert(err, qt.IsNil)
	c.Assert(fetched.Release.Generation, qt.Equals, "gen-2")
	c.Assert(string(fetched.Specification), qt.Equals, specification)
	// The digest the publish reported is the one the fetch resolved. A
	// promotion carries the digest rather than the tag, so a publish reporting
	// something a reader does not arrive at is the failure this crosses.
	c.Assert(fetched.Digest, qt.Equals, published.Descriptor.Digest.String())
	c.Assert(published.Reference.String(), qt.Equals, reference)
}
