package oci

// selectReferrer and selectPayload are the whole point of the fetch verb and
// they are pure, so they are tested here rather than through a live registry.
// The rule they encode — never choose between candidates — is the one
// stokaro/ptah#1143 asked for, and a test that had to publish two referrers to
// a real registry to check it would not be run often enough to hold it.

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func referrer(letter, artifactType string) ociartifact.DiscoveredReferrer {
	return ociartifact.DiscoveredReferrer{
		Descriptor: ocispec.Descriptor{
			ArtifactType: artifactType,
			Digest:       digest.Digest("sha256:" + strings.Repeat(letter, 64)),
		},
		Source: ociartifact.ReferrerSourceBoth,
	}
}

func TestSelectReferrer_FetchesTheOnlyCandidate(t *testing.T) {
	c := qt.New(t)
	only := referrer("a", ociartifact.DeploymentArtifactType)

	got, err := selectReferrer("oci://registry.invalid/acme/db:latest",
		[]ociartifact.DiscoveredReferrer{only}, fetchOptions{filter: "deployment"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, only.Descriptor.Digest.String())
}

// TestSelectReferrer_RefusesToChooseBetweenSeveral is the behavior the issue
// named: silently taking "the latest" is what must not happen. The assertion is
// on the refusal AND on the digests being printed, because a refusal that does
// not say what it was choosing between leaves the caller with no next step.
func TestSelectReferrer_RefusesToChooseBetweenSeveral(t *testing.T) {
	c := qt.New(t)
	first := referrer("a", ociartifact.DeploymentArtifactType)
	second := referrer("b", ociartifact.DeploymentArtifactType)

	_, err := selectReferrer("oci://registry.invalid/acme/db:latest",
		[]ociartifact.DiscoveredReferrer{first, second}, fetchOptions{filter: "deployment"})

	c.Assert(err, qt.ErrorMatches, `(?s).*has 2 deployment referrers and this command does not choose between them.*`)
	c.Assert(err.Error(), qt.Contains, first.Descriptor.Digest.String())
	c.Assert(err.Error(), qt.Contains, second.Descriptor.Digest.String())
}

func TestSelectReferrer_NamedDigestWins(t *testing.T) {
	c := qt.New(t)
	first := referrer("a", ociartifact.DeploymentArtifactType)
	second := referrer("b", ociartifact.DeploymentArtifactType)
	wanted := second.Descriptor.Digest.String()

	got, err := selectReferrer("oci://registry.invalid/acme/db:latest",
		[]ociartifact.DiscoveredReferrer{first, second},
		fetchOptions{filter: "deployment", digest: wanted})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, wanted)
}

func TestSelectReferrer_FailurePath(t *testing.T) {
	t.Run("no candidates", func(t *testing.T) {
		c := qt.New(t)

		_, err := selectReferrer("oci://registry.invalid/acme/db:latest", nil, fetchOptions{filter: "lint"})

		c.Assert(err, qt.ErrorMatches, `.*has no lint referrer to fetch`)
	})

	t.Run("named digest is not attached here", func(t *testing.T) {
		c := qt.New(t)
		present := referrer("a", ociartifact.LintArtifactType)

		_, err := selectReferrer("oci://registry.invalid/acme/db:latest",
			[]ociartifact.DiscoveredReferrer{present},
			fetchOptions{filter: "all", digest: "sha256:b"})

		c.Assert(err, qt.ErrorMatches, `.*has no attached referrer with digest sha256:b`)
	})
}

func TestSelectPayload_WritesTheOnlyFile(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{"deployment.json": {Data: []byte(`{"applied":1}`)}}

	got, err := selectPayload(fsys, "sha256:a", "")

	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, `{"applied":1}`)
}

func TestSelectPayload_RefusesToChooseBetweenFiles(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"deployment.json": {Data: []byte(`{"applied":1}`)},
		"extra.json":      {Data: []byte(`{}`)},
	}

	_, err := selectPayload(fsys, "sha256:a", "")

	c.Assert(err, qt.ErrorMatches, `.*carries 2 files and this command does not choose between them.*`)
	c.Assert(err.Error(), qt.Contains, "deployment.json")
	c.Assert(err.Error(), qt.Contains, "extra.json")
}

func TestSelectPayload_NamedFileWins(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"deployment.json": {Data: []byte(`{"applied":1}`)},
		"extra.json":      {Data: []byte(`{"other":true}`)},
	}

	got, err := selectPayload(fsys, "sha256:a", "extra.json")

	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, `{"other":true}`)
}

func TestSelectPayload_FailurePath(t *testing.T) {
	t.Run("no filesystem", func(t *testing.T) {
		c := qt.New(t)

		_, err := selectPayload(nil, "sha256:a", "")

		c.Assert(err, qt.ErrorMatches, `referrer sha256:a carries no files`)
	})

	t.Run("named file is absent", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{"deployment.json": {Data: []byte(`{}`)}}

		_, err := selectPayload(fsys, "sha256:a", "lint.json")

		c.Assert(err, qt.ErrorMatches, `referrer sha256:a carries no file "lint.json"; it carries deployment.json`)
	})
}
