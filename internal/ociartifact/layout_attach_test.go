package ociartifact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/internal/ociartifact"
)

const evidenceArtifactType = "application/vnd.ptah.evidence.v1+json"

// pushLayoutSubject writes a subject artifact into a fresh layout directory and
// returns the layout reference naming it.
func pushLayoutSubject(c *qt.C, ctx context.Context) string {
	c.Helper()
	ref := "oci-layout://" + filepath.Join(c.TB.TempDir(), "layout") + ":v1"
	store, tag, err := ociartifact.OpenLayout(ref)
	c.Assert(err, qt.IsNil)
	_, err = ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migrations/0001.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{tag},
	})
	c.Assert(err, qt.IsNil)
	return ref
}

// An air-gapped producer publishes a release to a directory and then attaches
// its verification evidence to that release. Before this, the publish half
// worked and the attachment half had no layout resolution at all.
// See stokaro/ptah#2839.
func TestAttach_IntoAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)

	attached, err := client.Attach(ctx, ref, fstest.MapFS{
		"evidence.json": {Data: []byte(`{"verified":true}`)},
	}, ociartifact.AttachmentOptions{ArtifactType: evidenceArtifactType})

	c.Assert(err, qt.IsNil)
	c.Assert(attached.Descriptor.Digest.String(), qt.Not(qt.Equals), "")
}

// The attachment has to be discoverable by something that did not make it.
// A layout carried across a gap is opened fresh on the other side, so the
// assertion that matters is made through a new reader of the directory rather
// than through the store that wrote it.
func TestAttach_IntoAnImageLayoutIsDiscoverableAfterReopen(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	attached, err := client.Attach(ctx, ref, fstest.MapFS{
		"evidence.json": {Data: []byte(`{"verified":true}`)},
	}, ociartifact.AttachmentOptions{ArtifactType: evidenceArtifactType})
	c.Assert(err, qt.IsNil)

	_, found, err := client.DiscoverReferrers(ctx, ref, evidenceArtifactType)

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Descriptor.Digest, qt.Equals, attached.Descriptor.Digest)
	// The artifact type has to arrive with it. A reopened store hands back a
	// predecessor descriptor with the field empty, so what a caller reads here
	// is what the manifest declared rather than what the graph remembered.
	c.Assert(found[0].Descriptor.ArtifactType, qt.Equals, evidenceArtifactType)
}

// A policy demanding the referrers index is satisfied by a directory, and this
// is the decision the issue asked to be made deliberately. index.json records
// the attachment whether or not it was tagged, so the guarantee the policy
// asks for -- discovery without guessing a tag -- is one the layout keeps.
func TestAttach_RequiredIndexPolicyAcceptsAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)

	attached, err := client.Attach(ctx, ref, fstest.MapFS{
		"evidence.json": {Data: []byte(`{"verified":true}`)},
	}, ociartifact.AttachmentOptions{
		ArtifactType: evidenceArtifactType,
		Policy:       ociartifact.ReferrerPolicyRequiredAPI,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(attached.Descriptor.Digest.String(), qt.Not(qt.Equals), "")
	// No durable tag, because an index-requiring policy writes none. The
	// attachment is discoverable anyway, which is the whole basis for
	// accepting the layout rather than refusing it.
	c.Assert(attached.Tags, qt.HasLen, 0)
}

// The control the issue asked for, and the first version of it was not one.
//
// Accepting a layout under a policy that demands the referrers index must not
// be reached by weakening the demand. An unreachable address proves nothing
// here: the run fails at the connection, so the assertion is satisfied whether
// or not the policy still refuses anything. This registry answers every
// request and serves no referrers index, which leaves the policy as the only
// thing that can refuse.
func TestAttach_RequiredIndexPolicyStillRefusesARegistryWithoutTheIndex(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	subjectDigest := digest.FromBytes(body)
	server := httptest.NewServer(registryWithoutReferrersIndex(body, subjectDigest))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)

	// AttachResolved, because the policy is checked before the subject is
	// fetched and this control is about the policy alone. Reaching it through
	// Attach would need the fake registry to serve a real manifest first, and
	// a failure there would satisfy the assertion for the wrong reason.
	subject := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    subjectDigest,
		Size:      int64(len(body)),
	}
	_, err = client.AttachResolved(ctx, "oci://"+host+"/acme/migrations:v1", subject, fstest.MapFS{
		"evidence.json": {Data: []byte(`{"verified":true}`)},
	}, ociartifact.AttachmentOptions{
		ArtifactType: evidenceArtifactType,
		Policy:       ociartifact.ReferrerPolicyRequiredAPI,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, ociartifact.ErrReferrerIndexRequired)
}

// A layout attachment must not report something that merely points at the
// subject. An image index listing the subject as a member is a predecessor and
// not a referrer, and the store's graph does not distinguish them -- measured,
// it returns both. This is what keeps the filter from widening back into the
// predecessor list it is derived from.
func TestDiscoverReferrers_InALayoutIgnoresANonReferrerPredecessor(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	attached, err := client.Attach(ctx, ref, fstest.MapFS{
		"evidence.json": {Data: []byte(`{"verified":true}`)},
	}, ociartifact.AttachmentOptions{ArtifactType: evidenceArtifactType})
	c.Assert(err, qt.IsNil)
	subject, _, err := client.DiscoverReferrers(ctx, ref, "")
	c.Assert(err, qt.IsNil)
	writeLayoutIndexOver(c, ctx, ref, subject)

	_, found, err := client.DiscoverReferrers(ctx, ref, "")

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Descriptor.Digest, qt.Equals, attached.Descriptor.Digest)
}

// writeLayoutIndexOver pushes an image index that lists subject as a member.
// It references the subject and is not a referrer of it.
func writeLayoutIndexOver(
	c *qt.C, ctx context.Context, ref string, subject ocispec.Descriptor,
) {
	c.Helper()
	store, _, err := ociartifact.OpenLayout(ref)
	c.Assert(err, qt.IsNil)
	index := ocispec.Index{
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{subject},
	}
	index.SchemaVersion = 2
	body, err := json.Marshal(index)
	c.Assert(err, qt.IsNil)
	c.Assert(store.Push(ctx, ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageIndex,
		Digest:    digest.FromBytes(body),
		Size:      int64(len(body)),
	}, bytes.NewReader(body)), qt.IsNil)
}

// An index can be a referrer, and the previous test proves only that one which
// is not gets ignored. Reading indexes has to be exercised by an index that
// carries a subject, or dropping them from the readable set would look correct.
func TestDiscoverReferrers_InALayoutReadsAnIndexThatCarriesASubject(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	subject, _, err := client.DiscoverReferrers(ctx, ref, "")
	c.Assert(err, qt.IsNil)
	attached := writeLayoutIndexReferrer(c, ctx, ref, subject)

	_, found, err := client.DiscoverReferrers(ctx, ref, "")

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Descriptor.Digest, qt.Equals, attached)
}

// A manifest that declares no artifactType is named by its config media type,
// which is what the image-spec says and what a manifest written before
// artifactType existed relies on.
func TestDiscoverReferrers_InALayoutNamesAManifestByItsConfigMediaType(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pushLayoutSubject(c, ctx)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	subject, _, err := client.DiscoverReferrers(ctx, ref, "")
	c.Assert(err, qt.IsNil)
	const configType = "application/vnd.ptah.legacy-evidence.v1+json"
	attached := writeLayoutBareManifestReferrer(c, ctx, ref, subject, configType)

	_, found, err := client.DiscoverReferrers(ctx, ref, configType)

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Descriptor.Digest, qt.Equals, attached)
}

// writeLayoutIndexReferrer pushes an image index whose subject is the given
// descriptor, and returns its digest.
func writeLayoutIndexReferrer(
	c *qt.C, ctx context.Context, ref string, subject ocispec.Descriptor,
) digest.Digest {
	c.Helper()
	index := ocispec.Index{
		MediaType:    ocispec.MediaTypeImageIndex,
		ArtifactType: evidenceArtifactType,
		Subject:      &subject,
	}
	index.SchemaVersion = 2
	return pushLayoutJSON(c, ctx, ref, ocispec.MediaTypeImageIndex, index)
}

// writeLayoutBareManifestReferrer pushes a manifest that carries a subject and
// declares no artifactType, naming itself only through its config media type.
func writeLayoutBareManifestReferrer(
	c *qt.C, ctx context.Context, ref string, subject ocispec.Descriptor, configType string,
) digest.Digest {
	c.Helper()
	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    ocispec.Descriptor{MediaType: configType, Digest: digest.FromString("{}"), Size: 2},
		Subject:   &subject,
	}
	manifest.SchemaVersion = 2
	return pushLayoutJSON(c, ctx, ref, ocispec.MediaTypeImageManifest, manifest)
}

// pushLayoutJSON writes one JSON document into the layout under mediaType.
func pushLayoutJSON(
	c *qt.C, ctx context.Context, ref, mediaType string, document any,
) digest.Digest {
	c.Helper()
	store, _, err := ociartifact.OpenLayout(ref)
	c.Assert(err, qt.IsNil)
	body, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	desc := ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(body),
		Size:      int64(len(body)),
	}
	c.Assert(store.Push(ctx, desc, bytes.NewReader(body)), qt.IsNil)
	return desc.Digest
}

// registryWithoutReferrersIndex answers every request except the referrers
// endpoint, so a probe reaches its question and gets "no index" for an answer
// rather than a transport failure.
//
// It lives outside the test function because it branches, and a conditional
// inside a Test* function is what scripts/check-test-style.sh refuses. The
// branching is the fake server's behavior, not the test's assertion strategy.
func registryWithoutReferrersIndex(body []byte, subject digest.Digest) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/referrers/") {
			http.NotFound(w, r)
			return
		}
		if !strings.Contains(r.URL.Path, "/manifests/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", subject.String())
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	}
}
