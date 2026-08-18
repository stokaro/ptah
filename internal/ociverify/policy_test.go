package ociverify_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociverify"
)

func writePolicy(c *qt.C, contents string) string {
	c.Helper()
	path := filepath.Join(c.TB.TempDir(), "policy.yaml")
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	return path
}

func TestLoadPolicy_HappyPath(t *testing.T) {
	c := qt.New(t)
	path := writePolicy(c, `
version: 1
require_digest_pin: true
artifact_types:
  - application/vnd.stokaro.ptah.migration.v1
require_annotations:
  - org.opencontainers.image.revision
require_signature: true
`)

	policy, err := ociverify.LoadPolicy(path)

	c.Assert(err, qt.IsNil)
	c.Assert(policy.RequireDigestPin, qt.IsTrue)
	c.Assert(policy.RequireSignature, qt.IsTrue)
	c.Assert(policy.ArtifactTypes, qt.DeepEquals,
		[]string{"application/vnd.stokaro.ptah.migration.v1"})
	c.Assert(policy.RequireAnnotations, qt.DeepEquals,
		[]string{"org.opencontainers.image.revision"})
}

// TestLoadPolicy_RefusesAPolicyThatGatesNothing separates the two ways to have
// no policy. Not passing one means the operator did not ask for a gate; passing
// an empty one means they believe they have one, and only the second is worth
// failing.
func TestLoadPolicy_RefusesAPolicyThatGatesNothing(t *testing.T) {
	c := qt.New(t)
	path := writePolicy(c, "version: 1\n")

	_, err := ociverify.LoadPolicy(path)

	c.Assert(err, qt.ErrorMatches, `.*declares no requirement, so every artifact would pass.*`)
}

func TestLoadPolicy_FailurePath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
		match    string
	}{
		{
			name:     "an unknown version is refused rather than interpreted",
			contents: "version: 2\nrequire_digest_pin: true\n",
			match:    `.*unsupported version 2: expected 1`,
		},
		{
			name:     "an unknown field is refused rather than ignored",
			contents: "version: 1\nrequire_digest_pin: true\nrequire_unicorn: true\n",
			match:    `(?s).*field require_unicorn not found.*`,
		},
		{
			name:     "an empty annotation name",
			contents: "version: 1\nrequire_annotations:\n  - \"\"\n",
			match:    `.*require_annotations contains an empty name`,
		},
		{
			name:     "an empty artifact type",
			contents: "version: 1\nartifact_types:\n  - \"\"\n",
			match:    `.*artifact_types contains an empty type`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			path := writePolicy(c, tc.contents)

			_, err := ociverify.LoadPolicy(path)

			c.Assert(err, qt.ErrorMatches, tc.match)
		})
	}

	t.Run("an absent file", func(t *testing.T) {
		c := qt.New(t)

		_, err := ociverify.LoadPolicy(filepath.Join(c.TB.TempDir(), "missing.yaml"))

		c.Assert(err, qt.ErrorMatches, "read the verification policy: .*")
	})
}

func TestDefaultSignatureArtifactTypes_CoverTheCommonSigners(t *testing.T) {
	c := qt.New(t)

	c.Assert(ociverify.DefaultSignatureArtifactTypes, qt.Contains,
		"application/vnd.dev.cosign.artifact.sig.v1+json")
	c.Assert(ociverify.DefaultSignatureArtifactTypes, qt.Contains,
		"application/vnd.cncf.notary.signature")
}
