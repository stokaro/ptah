package embedspec_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedspec"
)

// TestParsePublished_ReadsWhatTheReleaseRecorded is the receiving half of a
// promotion: an environment that has never seen the operator's file runs the
// bytes the release carried.
func TestParsePublished_ReadsWhatTheReleaseRecorded(t *testing.T) {
	c := qt.New(t)
	published, err := embedspec.Parse([]byte(complete), "articles.yaml")
	c.Assert(err, qt.IsNil)

	loaded, err := embedspec.ParsePublished(
		loadedDocument(c, published), "oci://example/articles:v2", published.Digest)

	c.Assert(err, qt.IsNil)
	// The same document, so the same generation. A promotion that produced a
	// different identity would recompute a corpus that is already correct.
	c.Assert(loaded.Digest, qt.Equals, published.Digest)
	c.Assert(loaded.Spec.Identity().Digest, qt.Equals, published.Spec.Identity().Digest)
}

// TestParsePublished_RefusesADocumentTheReleaseDoesNotDescribe is the check that
// makes the release record and its layer one artifact rather than two.
//
// Pulling by digest establishes that the artifact is the one the reference
// named. It cannot establish that the artifact agrees with itself, and a
// release whose record claims one specification while its layer carries another
// is what lets an approval, a cutover record and a verification all name a
// document nobody ran.
func TestParsePublished_RefusesADocumentTheReleaseDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	published, err := embedspec.Parse([]byte(complete), "articles.yaml")
	c.Assert(err, qt.IsNil)
	// One field the identity is taken over, so the substituted document is a
	// valid specification for a DIFFERENT generation -- which is the case a
	// parse error would not have caught.
	tampered := replaceOnce(complete, "input_fields: [title, body]", "input_fields: [body]")

	loaded, err := embedspec.ParsePublished(
		[]byte(tampered), "oci://example/articles:v2", published.Digest)

	c.Assert(err, qt.ErrorIs, embedspec.ErrDocumentMismatch)
	c.Assert(err, qt.ErrorMatches, `.*oci://example/articles:v2 carries [0-9a-f]{64} and the release records `+
		published.Digest+`.*`)
	c.Assert(loaded.Digest, qt.Equals, "")
}

// loadedDocument is the bytes a release would have carried.
//
// Taken from the parsed specification rather than restated, because that is
// what the publishing half writes: a fixture spelling the document again would
// pass whether or not Parse kept it.
func loadedDocument(c *qt.C, loaded embedspec.Loaded) []byte {
	c.Helper()
	c.Assert(loaded.Document, qt.Not(qt.HasLen), 0)
	return loaded.Document
}
