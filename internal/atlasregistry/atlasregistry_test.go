package atlasregistry_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasregistry"
)

// `atlas://` names a repository and a pointer with no registry host in it,
// because the vendor spelling assumes one hosted account. Ptah has none, so the
// reference resolves against a namespace the operator configures, and a run
// with none configured is refused rather than sent anywhere
// (stokaro/ptah#1210).

func TestResolve_MapsTheThreeDocumentedForms(t *testing.T) {
	tests := []struct {
		name          string
		raw           string
		wantOCI       string
		wantImmutable bool
	}{
		{name: "bare repository", raw: "atlas://app", wantOCI: "oci://ghcr.io/acme/app:latest"},
		{name: "tag", raw: "atlas://app?tag=production", wantOCI: "oci://ghcr.io/acme/app:production"},
		{
			name:          "version",
			raw:           "atlas://app?version=20260806123000",
			wantOCI:       "oci://ghcr.io/acme/app:20260806123000",
			wantImmutable: true,
		},
		{name: "nested repository", raw: "atlas://team/app?tag=stable", wantOCI: "oci://ghcr.io/acme/team/app:stable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

			resolved, err := atlasregistry.Resolve(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(resolved.OCI, qt.Equals, test.wantOCI)
			c.Assert(resolved.Immutable, qt.Equals, test.wantImmutable)
		})
	}
}

// TestResolve_VersionIsImmutableAndTagIsNot is the contract the issue states:
// a tag moves, a version does not. Ptah spells the second as a write-once tag,
// and this is where the two are told apart.
func TestResolve_VersionIsImmutableAndTagIsNot(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	tagged, tagErr := atlasregistry.Resolve("atlas://app?tag=v1")
	versioned, versionErr := atlasregistry.Resolve("atlas://app?version=v1")

	c.Assert(tagErr, qt.IsNil)
	c.Assert(versionErr, qt.IsNil)
	c.Assert(tagged.OCI, qt.Equals, versioned.OCI)
	c.Assert(tagged.Immutable, qt.IsFalse)
	c.Assert(versioned.Immutable, qt.IsTrue)
}

// TestResolve_FailsClosedWithoutANamespace is the rule that matters most: Ptah
// must never send an atlas:// reference to a hosted service as an implicit
// fallback, and a reference resolving to nothing would be worse than a refusal.
func TestResolve_FailsClosedWithoutANamespace(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "")

	_, err := atlasregistry.Resolve("atlas://app")

	c.Assert(err, qt.ErrorMatches, `atlas:// references require an OCI backing registry in Ptah: set PTAH_ATLAS_REGISTRY.*`)
	c.Assert(err.Error(), qt.Contains, "ghcr.io/acme")
	c.Assert(err.Error(), qt.Contains, "oci://")
}

// TestResolve_RefusesAnUndocumentedQueryParameter keeps the surface to what
// public documentation establishes. Ignoring an unknown parameter would resolve
// to a different artifact than the reference asked for and say nothing.
func TestResolve_RefusesAnUndocumentedQueryParameter(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	_, err := atlasregistry.Resolve("atlas://app?channel=beta")

	c.Assert(err, qt.ErrorMatches, `.*query parameter "channel", which has no documented meaning here.*`)
}

// TestResolve_RefusesTagAndVersionTogether holds the one combination that names
// two different artifacts.
func TestResolve_RefusesTagAndVersionTogether(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	_, err := atlasregistry.Resolve("atlas://app?tag=prod&version=20260806123000")

	c.Assert(err, qt.ErrorMatches, `.*names both tag "prod" and version "20260806123000".*`)
}

func TestResolve_RefusesAReferenceWithNoRepository(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	_, err := atlasregistry.Resolve("atlas://")

	c.Assert(err, qt.ErrorMatches, `.*names no repository`)
}

// TestResolve_TrimsNamespaceSlashes is the paper cut a configured value picks
// up from a copy-paste: a trailing slash must not produce a double one.
func TestResolve_TrimsNamespaceSlashes(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme/")

	resolved, err := atlasregistry.Resolve("atlas://app")

	c.Assert(err, qt.IsNil)
	c.Assert(resolved.OCI, qt.Equals, "oci://ghcr.io/acme/app:latest")
}

func TestIsReference_KeysOnTheScheme(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasregistry.IsReference("atlas://app"), qt.IsTrue)
	c.Assert(atlasregistry.IsReference("  ATLAS://app  "), qt.IsTrue)
	c.Assert(atlasregistry.IsReference("oci://ghcr.io/acme/app"), qt.IsFalse)
	c.Assert(atlasregistry.IsReference("atlas.hcl"), qt.IsFalse)
}
