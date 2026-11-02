package ociregistrytest_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/internal/ociartifact"
	"ptah.run/internal/ociregistrytest"
)

const repository = "acme/app"

// isolateCredentials keeps the client away from the developer's credential
// stores. The registry never challenges, so no credential is read; the client
// still opens the stores when it is built.
func isolateCredentials(c *qt.C) {
	c.Setenv("DOCKER_CONFIG", c.TempDir())
	c.Setenv("PTAH_OCI_CONFIG", filepath.Join(c.TempDir(), "registries.json"))
	c.Setenv("PTAH_OCI_CREDENTIAL_STORE", "file")
}

func push(c *qt.C, reference, body string, tags ...string) ociartifact.PushResult {
	c.Helper()
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	result, err := client.Push(c.Context(), reference, fstest.MapFS{
		"migration.sql": {Data: []byte(body)},
	}, ociartifact.PushOptions{ArtifactType: ociartifact.MigrationArtifactType, Tags: tags})
	c.Assert(err, qt.IsNil)
	return result
}

// A push through Ptah's own client lands, and what the registry holds under the
// tag is the manifest the client reports, byte for byte.
func TestRegistry_StoresWhatAClientPushes(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, repository)

	result := push(c, registry.Reference("v1"), "SELECT 1;\n")

	written, ok := registry.Written("v1")
	c.Assert(ok, qt.IsTrue)
	c.Assert(written, qt.Equals, result.Descriptor.Digest)
	body, mediaType, ok := registry.Manifest(written)
	c.Assert(ok, qt.IsTrue)
	c.Assert(digest.FromBytes(body), qt.Equals, written)
	c.Assert(mediaType, qt.Equals, ocispec.MediaTypeImageManifest)
	c.Assert(registry.PinnedReference(written), qt.Equals,
		"oci://"+registry.Host()+"/"+repository+"@"+written.String())

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	_, resolved, err := client.Resolve(c.Context(), registry.Reference("v1"))
	c.Assert(err, qt.IsNil)
	c.Assert(resolved.Digest, qt.Equals, written)
}

// A refused tag fails the push, after the manifest itself was stored by digest,
// which is the state a registry's tag-protection policy leaves behind.
func TestRegistry_RefuseTagFailsThePush(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, repository)
	registry.RefuseTag("v1")

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	result, err := client.Push(c.Context(), registry.Reference("v1"), fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{ArtifactType: ociartifact.MigrationArtifactType})

	var partial *ociartifact.PartialPushError
	c.Assert(err, qt.ErrorAs, &partial)
	c.Assert(partial.FailedTag, qt.Equals, "v1")
	c.Assert(err, qt.ErrorMatches, `(?s).*response status code 403: denied: tag "v1" is protected`)
	_, written := registry.Written("v1")
	c.Assert(written, qt.IsFalse)
	_, _, stored := registry.Manifest(result.Descriptor.Digest)
	c.Assert(stored, qt.IsTrue)
}

// RepointReads answers a read of the tag with another manifest, while Written
// still reports the one the push stored: the two answers a client that reads
// the tag back and one that kept its own descriptor would give.
func TestRegistry_RepointReadsSeparatesReadsFromWrites(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, repository)
	other := push(c, registry.Reference("other"), "SELECT 2;\n")
	registry.RepointReads("release", other.Descriptor.Digest)

	result := push(c, registry.Reference("release"), "SELECT 1;\n")

	written, ok := registry.Written("release")
	c.Assert(ok, qt.IsTrue)
	c.Assert(written, qt.Equals, result.Descriptor.Digest)
	c.Assert(written, qt.Not(qt.Equals), other.Descriptor.Digest)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	_, resolved, err := client.Resolve(c.Context(), registry.Reference("release"))
	c.Assert(err, qt.IsNil)
	c.Assert(resolved.Digest, qt.Equals, other.Descriptor.Digest)
}

// Every request is counted, so a test can show a refusal happened before the
// registry was reached.
func TestRegistry_CountsRequests(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, repository)
	c.Assert(registry.Requests(), qt.Equals, int64(0))

	push(c, registry.Reference("v1"), "SELECT 1;\n")

	c.Assert(registry.Requests() > 0, qt.IsTrue)
}
