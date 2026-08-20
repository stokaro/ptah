// Package schemaartifacttest serves a real schema artifact over the read half of
// the OCI distribution API, for tests that need a registry without a container.
//
// It is its own package rather than part of internal/testutils because it pulls
// the artifact and HCL dependency graphs in, and testutils sits in the
// dependency graph of the published testkit module -- a test helper should not
// widen what consumers of that module resolve.
package schemaartifacttest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaartifact"
)

// StartSchemaArtifactRegistry publishes db as a schema artifact and serves it
// over the read half of the OCI distribution API, returning the host:port the
// reference should name.
//
// It serves a REAL artifact rather than stubbing the pull, so the test exercises
// the client, the media types and the artifact-type check on the way through.
// A stub would agree with any pull the resolver made -- including one that
// fetched a migration directory and called it a schema.
func StartSchemaArtifactRegistry(c *qt.C, repository, tag string, db *goschema.Database) string {
	c.Helper()
	store := newRecordingTarget()
	_, err := schemaartifact.PushTo(c.TB.Context(), store, db, schemaartifact.PushOptions{Tags: []string{tag}})
	c.Assert(err, qt.IsNil)

	serve := func(writer http.ResponseWriter, wanted digest.Digest) {
		blob, ok := store.blob(wanted)
		if !ok {
			http.Error(writer, "not found", http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", blob.descriptor.MediaType)
		writer.Header().Set("Docker-Content-Digest", blob.descriptor.Digest.String())
		writer.Header().Set("Content-Length", strconv.Itoa(len(blob.data)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(blob.data)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v2/"+repository+"/manifests/{reference}", func(
		writer http.ResponseWriter, request *http.Request,
	) {
		descriptor, ok := store.tag(request.PathValue("reference"))
		if !ok {
			http.Error(writer, "manifest unknown", http.StatusNotFound)
			return
		}
		serve(writer, descriptor.Digest)
	})
	// Blobs are addressed by digest, which is not a tag: Resolve answers tags,
	// so a blob request has to be fetched by a descriptor built from the digest
	// in the path.
	mux.HandleFunc("/v2/"+repository+"/blobs/{digest}", func(
		writer http.ResponseWriter, request *http.Request,
	) {
		serve(writer, digest.Digest(request.PathValue("digest")))
	})

	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

// recordingTarget is the smallest oras.Target that keeps every pushed blob
// addressable by its digest.
//
// The in-memory content store cannot serve this role: its Fetch takes a full
// descriptor, and a registry request carries only the digest from the URL path
// — so a blob request could never be answered from it.
type recordingTarget struct {
	mu    sync.Mutex
	blobs map[digest.Digest]storedBlob
	tags  map[string]ocispec.Descriptor
}

type storedBlob struct {
	descriptor ocispec.Descriptor
	data       []byte
}

func newRecordingTarget() *recordingTarget {
	return &recordingTarget{
		blobs: make(map[digest.Digest]storedBlob),
		tags:  make(map[string]ocispec.Descriptor),
	}
}

func (t *recordingTarget) blob(wanted digest.Digest) (storedBlob, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	blob, ok := t.blobs[wanted]
	return blob, ok
}

func (t *recordingTarget) tag(reference string) (ocispec.Descriptor, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	descriptor, ok := t.tags[reference]
	return descriptor, ok
}

func (t *recordingTarget) Push(_ context.Context, expected ocispec.Descriptor, content io.Reader) error {
	data, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blobs[expected.Digest] = storedBlob{descriptor: expected, data: data}
	return nil
}

func (t *recordingTarget) Exists(_ context.Context, target ocispec.Descriptor) (bool, error) {
	_, ok := t.blob(target.Digest)
	return ok, nil
}

func (t *recordingTarget) Fetch(_ context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	blob, ok := t.blob(target.Digest)
	if !ok {
		return nil, errdef.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(blob.data)), nil
}

func (t *recordingTarget) Resolve(_ context.Context, reference string) (ocispec.Descriptor, error) {
	if descriptor, ok := t.tag(reference); ok {
		return descriptor, nil
	}
	if blob, ok := t.blob(digest.Digest(reference)); ok {
		return blob.descriptor, nil
	}
	return ocispec.Descriptor{}, errdef.ErrNotFound
}

func (t *recordingTarget) Tag(_ context.Context, target ocispec.Descriptor, reference string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tags[reference] = target
	return nil
}
