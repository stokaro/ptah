// Package ociregistrytest serves an in-process OCI distribution registry that
// accepts pushes, for tests that publish through Ptah's own registry client
// without a container.
//
// [ptah.run/internal/schemaartifacttest] serves one artifact it published
// itself over the read half of the API. This registry also answers the write
// half -- blob uploads and manifest PUTs -- so a test can run `ptah schema push`
// or `ptah migrations push` end to end and then read back what the registry
// holds, which is the only way to check that a command reported the manifest it
// actually wrote.
//
// It holds one repository. It verifies every digest it is sent, the way a real
// registry does, so a client that computed one wrong fails here rather than
// passing against a store that believed it.
package ociregistrytest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
)

// Registry is a running in-process registry serving one repository.
type Registry struct {
	host       string
	repository string
	requests   atomic.Int64

	mu        sync.Mutex
	blobs     map[digest.Digest][]byte
	manifests map[digest.Digest]storedManifest
	// written is what each manifest PUT stored under a reference, and tags is
	// what a read of that reference answers. They differ only for a tag passed
	// to RepointReads.
	written map[string]digest.Digest
	tags    map[string]digest.Digest
	// repointed holds the tags RepointReads fixed, which a PUT no longer moves.
	repointed map[string]bool
	refused   map[string]bool
	uploadID  int
}

type storedManifest struct {
	mediaType string
	body      []byte
}

// Start serves an empty registry holding repository and stops it when the test
// ends.
func Start(c *qt.C, repository string) *Registry {
	c.Helper()
	registry := &Registry{
		repository: repository,
		blobs:      make(map[digest.Digest][]byte),
		manifests:  make(map[digest.Digest]storedManifest),
		written:    make(map[string]digest.Digest),
		tags:       make(map[string]digest.Digest),
		repointed:  make(map[string]bool),
		refused:    make(map[string]bool),
	}
	prefix := "/v2/" + repository
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/{$}", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	// A GET pattern also answers HEAD, which is how a client asks whether a
	// blob exists or what a tag names.
	mux.HandleFunc("GET "+prefix+"/blobs/{digest}", registry.serveBlob)
	mux.HandleFunc("POST "+prefix+"/blobs/uploads/{$}", registry.startUpload)
	mux.HandleFunc("PUT "+prefix+"/blobs/uploads/{id}", registry.finishUpload)
	mux.HandleFunc("GET "+prefix+"/manifests/{reference}", registry.serveManifest)
	mux.HandleFunc("PUT "+prefix+"/manifests/{reference}", registry.storeManifest)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		registry.requests.Add(1)
		mux.ServeHTTP(writer, request)
	}))
	c.Cleanup(server.Close)
	registry.host = strings.TrimPrefix(server.URL, "http://")
	return registry
}

// Host returns the host:port an oci:// reference names to reach the registry.
func (r *Registry) Host() string {
	return r.host
}

// Reference returns the oci:// reference that names tag in the repository.
func (r *Registry) Reference(tag string) string {
	return "oci://" + r.host + "/" + r.repository + ":" + tag
}

// PinnedReference returns the oci:// reference that names dgst in the
// repository, in the form a digest pin takes.
func (r *Registry) PinnedReference(dgst digest.Digest) string {
	return "oci://" + r.host + "/" + r.repository + "@" + dgst.String()
}

// Requests returns how many HTTP requests the registry has answered.
func (r *Registry) Requests() int64 {
	return r.requests.Load()
}

// Written returns the digest of the last manifest a PUT stored under tag, and
// false when no PUT has. It is what the client wrote, whatever a read of the tag
// answers now.
func (r *Registry) Written(tag string) (digest.Digest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	dgst, ok := r.written[tag]
	return dgst, ok
}

// Manifest returns the bytes and media type of the manifest stored under dgst.
func (r *Registry) Manifest(dgst digest.Digest) (body []byte, mediaType string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	stored, ok := r.manifests[dgst]
	return stored.body, stored.mediaType, ok
}

// RefuseTag makes every manifest PUT under tag answer 403 DENIED, as a
// registry with a tag-protection policy does.
func (r *Registry) RefuseTag(tag string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refused[tag] = true
}

// RepointReads makes every read of tag answer dgst, whatever a PUT stores under
// it afterwards. It is another writer moving a movable tag the moment a push
// has written it: a client that reads the tag back reports that writer's
// manifest instead of its own.
func (r *Registry) RepointReads(tag string, dgst digest.Digest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tags[tag] = dgst
	r.repointed[tag] = true
}

func (r *Registry) serveBlob(writer http.ResponseWriter, request *http.Request) {
	dgst, err := digest.Parse(request.PathValue("digest"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "DIGEST_INVALID", err.Error())
		return
	}
	r.mu.Lock()
	body, ok := r.blobs[dgst]
	r.mu.Unlock()
	if !ok {
		writeError(writer, http.StatusNotFound, "BLOB_UNKNOWN", "blob unknown to registry")
		return
	}
	writeContent(writer, request, "application/octet-stream", dgst, body)
}

func (r *Registry) startUpload(writer http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	r.uploadID++
	id := r.uploadID
	r.mu.Unlock()
	writer.Header().Set("Location", "/v2/"+r.repository+"/blobs/uploads/"+strconv.Itoa(id))
	writer.WriteHeader(http.StatusAccepted)
}

func (r *Registry) finishUpload(writer http.ResponseWriter, request *http.Request) {
	dgst, err := digest.Parse(request.URL.Query().Get("digest"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "DIGEST_INVALID", err.Error())
		return
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", err.Error())
		return
	}
	if dgst.Algorithm().FromBytes(body) != dgst {
		writeError(writer, http.StatusBadRequest, "DIGEST_INVALID", "uploaded content does not match "+dgst.String())
		return
	}
	r.mu.Lock()
	r.blobs[dgst] = body
	r.mu.Unlock()
	writer.Header().Set("Docker-Content-Digest", dgst.String())
	writer.WriteHeader(http.StatusCreated)
}

func (r *Registry) serveManifest(writer http.ResponseWriter, request *http.Request) {
	reference := request.PathValue("reference")
	r.mu.Lock()
	dgst, tagged := r.tags[reference]
	if !tagged {
		dgst = digest.Digest(reference)
	}
	stored, ok := r.manifests[dgst]
	r.mu.Unlock()
	if !ok {
		writeError(writer, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown to registry")
		return
	}
	writeContent(writer, request, stored.mediaType, dgst, stored.body)
}

func (r *Registry) storeManifest(writer http.ResponseWriter, request *http.Request) {
	reference := request.PathValue("reference")
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "MANIFEST_INVALID", err.Error())
		return
	}
	dgst := digest.FromBytes(body)
	named, err := digest.Parse(reference)
	byDigest := err == nil
	if byDigest && named != dgst {
		writeError(writer, http.StatusBadRequest, "DIGEST_INVALID", "manifest does not match "+named.String())
		return
	}
	if !json.Valid(body) {
		writeError(writer, http.StatusBadRequest, "MANIFEST_INVALID", "manifest is not JSON")
		return
	}

	r.mu.Lock()
	if r.refused[reference] {
		r.mu.Unlock()
		writeError(writer, http.StatusForbidden, "DENIED", fmt.Sprintf("tag %q is protected", reference))
		return
	}
	r.manifests[dgst] = storedManifest{mediaType: request.Header.Get("Content-Type"), body: body}
	if !byDigest {
		r.written[reference] = dgst
		if !r.repointed[reference] {
			r.tags[reference] = dgst
		}
	}
	r.mu.Unlock()

	writer.Header().Set("Docker-Content-Digest", dgst.String())
	writer.Header().Set("Location", "/v2/"+r.repository+"/manifests/"+dgst.String())
	writer.WriteHeader(http.StatusCreated)
}

// writeContent answers a GET with body and a HEAD with its headers alone. The
// length is set explicitly so a HEAD carries it too: a client sizes the
// descriptor it resolves from that header.
func writeContent(writer http.ResponseWriter, request *http.Request, mediaType string, dgst digest.Digest, body []byte) {
	writer.Header().Set("Content-Type", mediaType)
	writer.Header().Set("Docker-Content-Digest", dgst.String())
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = writer.Write(body)
	}
}

// writeError answers in the error format of the distribution specification,
// which is what a client parses into the message it reports.
func writeError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"errors": []map[string]string{{"code": code, "message": message}},
	})
}
