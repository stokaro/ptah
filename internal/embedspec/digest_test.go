package embedspec_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedspec"
)

// TestParse_TheDocumentDigestFollowsTheBytes is the half of the answer the
// generation identity does not give.
//
// The identity covers what decides the vectors, so an edit to a name, a
// description or an index option leaves it alone -- correctly, because none of
// them makes a corpus incomparable. Which document proposed a change is a
// different question, and it is the one somebody reading a release six months
// later is asking. Each row below is an edit the identity ignores.
func TestParse_TheDocumentDigestFollowsTheBytes(t *testing.T) {
	tests := []struct {
		name string
		edit func(string) string
	}{
		{
			name: "a different name",
			edit: func(s string) string { return replaceOnce(s, "name: articles v2", "name: whatever") },
		},
		{
			name: "different index options",
			edit: func(s string) string { return replaceOnce(s, `m: "16"`, `m: "64"`) },
		},
		{
			name: "a trailing blank line",
			edit: func(s string) string { return s + "\n" },
		},
	}

	base := mustParse(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			edited, err := embedspec.Parse([]byte(test.edit(complete)), "spec.yaml")

			c.Assert(err, qt.IsNil)
			// The generation is the same, which is what makes the row worth
			// having: an assertion that only fired where the identity fired
			// too would be measuring the identity.
			c.Assert(edited.Spec.Identity().Digest, qt.Equals, base.Spec.Identity().Digest)
			c.Assert(edited.Digest, qt.Not(qt.Equals), base.Digest)
		})
	}
}

// TestParse_TheSameDocumentDigestsTheSame is the control. A content address
// that moved on its own would make every release look like a new proposal, and
// every row above would still pass.
func TestParse_TheSameDocumentDigestsTheSame(t *testing.T) {
	c := qt.New(t)

	first, err := embedspec.Parse([]byte(complete), "spec.yaml")
	c.Assert(err, qt.IsNil)
	// Read under another name, because the digest is the document's and not the
	// path's: two checkouts of one file are one proposal.
	second, err := embedspec.Parse([]byte(complete), "somewhere/else.yaml")
	c.Assert(err, qt.IsNil)

	c.Assert(first.Digest, qt.Equals, second.Digest)
	c.Assert(first.Digest, qt.HasLen, 64)
	c.Assert(first.Digest, qt.Not(qt.Equals), first.Spec.Identity().Digest)
}
