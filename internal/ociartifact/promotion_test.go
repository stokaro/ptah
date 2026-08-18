package ociartifact_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func TestSortedTags_IsStableAndDoesNotMutate(t *testing.T) {
	c := qt.New(t)
	input := []string{"production", "latest", "staging"}

	got := ociartifact.SortedTags(input)

	c.Assert(got, qt.DeepEquals, []string{"latest", "production", "staging"})
	c.Assert(input, qt.DeepEquals, []string{"production", "latest", "staging"},
		qt.Commentf("the caller's slice must survive being sorted for output"))
}

// TestRetag_RefusesBeforeTouchingTheRegistry pins that the argument checks run
// first. A promotion that reaches the network before noticing it was told
// nothing to do has already spent an authenticated round trip on a request it
// was always going to refuse.
func TestRetag_RefusesBeforeTouchingTheRegistry(t *testing.T) {
	// The two refusals are different answers and the command should not
	// collapse them: one caller asked for nothing, the other asked for
	// something that is not a tag, and only the second is a typo.
	for _, tc := range []struct {
		name  string
		tags  []string
		match string
	}{
		{name: "no tags at all", tags: nil, match: "a tag to move is required"},
		{name: "an empty tag is invalid, not absent", tags: []string{""}, match: `invalid OCI tag .*`},
		{name: "a blank tag is invalid, not absent", tags: []string{"  "}, match: `invalid OCI tag .*`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
			c.Assert(err, qt.IsNil)

			_, applied, err := client.Retag(context.Background(),
				"oci://registry.invalid/acme/db:latest", tc.tags)

			c.Assert(err, qt.ErrorMatches, tc.match)
			c.Assert(applied, qt.HasLen, 0)
		})
	}
	t.Run("the empty case is the sentinel", func(t *testing.T) {
		c := qt.New(t)
		client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
		c.Assert(err, qt.IsNil)

		_, _, err = client.Retag(context.Background(), "oci://registry.invalid/acme/db:latest", nil)

		c.Assert(err, qt.ErrorIs, ociartifact.ErrTagRequired)
	})
}

// TestCopyArtifact_RefusesADigestDestination keeps the copy honest about what a
// destination is. A digest names content that already exists, so a copy that
// accepted one would be claiming to create something at an address that cannot
// be written to.
func TestCopyArtifact_RefusesADigestDestination(t *testing.T) {
	c := qt.New(t)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	digest := "oci://registry.invalid/acme/db@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	_, err = client.CopyArtifact(context.Background(),
		"oci://registry.invalid/acme/db:latest", digest, ociartifact.ArtifactCopyOptions{})

	c.Assert(err, qt.ErrorIs, ociartifact.ErrDigestPush)
}

func TestCopyArtifact_FailurePath(t *testing.T) {
	t.Run("unparsable source names which side", func(t *testing.T) {
		c := qt.New(t)
		client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
		c.Assert(err, qt.IsNil)

		_, err = client.CopyArtifact(context.Background(), "::not a reference::",
			"oci://registry.invalid/acme/db:latest", ociartifact.ArtifactCopyOptions{})

		c.Assert(err, qt.ErrorMatches, "parse source: .*")
	})

	t.Run("unparsable destination names which side", func(t *testing.T) {
		c := qt.New(t)
		client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
		c.Assert(err, qt.IsNil)

		_, err = client.CopyArtifact(context.Background(),
			"oci://registry.invalid/acme/db:latest", "::not a reference::",
			ociartifact.ArtifactCopyOptions{})

		c.Assert(err, qt.ErrorMatches, "parse destination: .*")
	})
}
