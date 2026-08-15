package migrateup

// White-box testing required: this file hosts the in-process OCI registry
// harness (ociMemoryStore, startOCITestRegistry, pushProvenanceArtifact and
// runUpInternal) that oci_digest_pin_internal_test.go also drives, and that
// file needs package-internal access of its own. Keeping one harness in one
// package is what stops two in-process registries drifting into disagreeing
// about what a tag resolves to.
//
// The provenance PREDICATE this file used to reach into is no longer private:
// it is [go.5x5.cz/ptah/cmd/internal/migrationsource.MutableTagSumWarning],
// exported when `migrations down` and `status` gained --verify-sum and needed
// the same qualifier (stokaro/ptah#928 item 4). Its own rows moved with it, to
// cmd/internal/migrationsource, because a predicate reachable through an
// exported API no longer justifies a white-box test. What stays here is the
// wiring half, which drives the real apply path.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

// ociStoredBlob is one content-addressed object the in-process registry serves.
type ociStoredBlob struct {
	descriptor ocispec.Descriptor
	data       []byte
}

// ociMemoryStore is the minimal oras.Target the in-process registry is built
// from: digest-addressed content plus a mutable tag table, which is exactly the
// registry property this test exercises.
type ociMemoryStore struct {
	mu    sync.Mutex
	blobs map[digest.Digest]ociStoredBlob
	tags  map[string]ocispec.Descriptor
}

func newOCIMemoryStore() *ociMemoryStore {
	return &ociMemoryStore{
		blobs: map[digest.Digest]ociStoredBlob{},
		tags:  map[string]ocispec.Descriptor{},
	}
}

func (s *ociMemoryStore) Push(_ context.Context, expected ocispec.Descriptor, content io.Reader) error {
	data, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[expected.Digest] = ociStoredBlob{descriptor: expected, data: data}
	return nil
}

func (s *ociMemoryStore) Exists(_ context.Context, target ocispec.Descriptor) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.blobs[target.Digest]
	return ok, nil
}

func (s *ociMemoryStore) Fetch(_ context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, ok := s.blobs[target.Digest]
	if !ok {
		return nil, errdef.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(blob.data)), nil
}

func (s *ociMemoryStore) Tag(_ context.Context, target ocispec.Descriptor, reference string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tags[reference] = target
	return nil
}

func (s *ociMemoryStore) Resolve(_ context.Context, reference string) (ocispec.Descriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tagged, ok := s.tags[reference]; ok {
		return tagged, nil
	}
	blob, ok := s.blobs[digest.Digest(reference)]
	if !ok {
		return ocispec.Descriptor{}, errdef.ErrNotFound
	}
	return blob.descriptor, nil
}

func (s *ociMemoryStore) serveManifest(writer http.ResponseWriter, request *http.Request) {
	descriptor, err := s.Resolve(request.Context(), request.PathValue("reference"))
	if err != nil {
		http.Error(writer, "manifest unknown", http.StatusNotFound)
		return
	}
	s.mu.Lock()
	blob := s.blobs[descriptor.Digest]
	s.mu.Unlock()
	writer.Header().Set("Content-Type", descriptor.MediaType)
	writer.Header().Set("Docker-Content-Digest", descriptor.Digest.String())
	writer.Header().Set("Content-Length", strconv.Itoa(len(blob.data)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(blob.data)
}

func (s *ociMemoryStore) serveBlob(writer http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	blob, ok := s.blobs[digest.Digest(request.PathValue("digest"))]
	s.mu.Unlock()
	if !ok {
		http.Error(writer, "blob unknown", http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Docker-Content-Digest", blob.descriptor.Digest.String())
	writer.Header().Set("Content-Length", strconv.Itoa(len(blob.data)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(blob.data)
}

// startOCITestRegistry serves store over the read half of the OCI distribution
// API and returns its host:port, so `ptah migrations up --plain-http` resolves
// a real oci:// reference without a container.
func startOCITestRegistry(c *qt.C, store *ociMemoryStore, repository string) string {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v2/"+repository+"/manifests/{reference}", store.serveManifest)
	mux.HandleFunc("/v2/"+repository+"/blobs/{digest}", store.serveBlob)
	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func writeHashedProvenanceDir(c *qt.C, table string) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_create_" + table + ".up.sql":   "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY);\n",
		"0000000001_create_" + table + ".down.sql": "DROP TABLE " + table + ";\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	return dir
}

func pushProvenanceArtifact(c *qt.C, store *ociMemoryStore, dir, tag string) string {
	c.Helper()
	result, err := migrationartifact.PushTo(
		context.Background(),
		store,
		os.DirFS(dir),
		migrationartifact.PushOptions{
			Tags:        []string{tag},
			DirFormat:   migrator.MigrationDirFormatPtah,
			Annotations: map[string]string{ocispec.AnnotationCreated: "2026-08-01T00:00:00Z"},
		},
	)
	c.Assert(err, qt.IsNil)
	return result.Descriptor.Digest.String()
}

func runUpInternal(args ...string) (string, error) {
	cmd := NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// provenanceReferences is one pushed artifact named the two ways a caller can
// name it: the movable tag, and the digest that tag resolved to.
type provenanceReferences struct {
	tag      string
	digest   string
	resolved string
}

// pushProvenanceReferences serves one artifact from an in-process registry and
// returns both spellings of it. Both tests below start here, so the tag row and
// the digest row are measured against the SAME bytes — which is what makes the
// difference between their outputs a property of the reference and not of the
// content.
func pushProvenanceReferences(c *qt.C) provenanceReferences {
	c.Helper()
	const repository = "ptah/provenance"
	store := newOCIMemoryStore()
	host := startOCITestRegistry(c, store, repository)
	resolved := pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "widgets"), "release")
	return provenanceReferences{
		tag:      "oci://" + host + "/" + repository + ":release",
		digest:   "oci://" + host + "/" + repository + "@" + resolved,
		resolved: resolved,
	}
}

// applyProvenanceReference runs the real apply path against reference and
// returns its output, having asserted what every reference owes regardless of
// its spelling: the run succeeds, the sum claim is printed, and the database
// moved. A warning qualifies that claim; it never replaces it and never changes
// the exit status.
func applyProvenanceReference(c *qt.C, reference string) string {
	c.Helper()
	dbPath := filepath.Join(c.TempDir(), "provenance.db")

	out, err := runUpInternal(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", reference,
		"--verify-sum",
		"--plain-http",
		"--skip-report",
		"--verbose",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "ptah.sum verified: migrations directory is intact")
	c.Assert(out, qt.Contains, "Database is now at version: 1")
	return out
}

// TestMigrateUp_OCISumProvenanceWarnsOnAMovableTag runs the real apply path
// against an in-process registry. It is the wiring half of stokaro/ptah#944:
// measurement showed `ptah.sum verified: migrations directory is intact`
// printed byte-identically for a tag whose content had been swapped and for the
// digest that pinned the reviewed bytes, so a tag must say what its sum does
// and does not prove.
func TestMigrateUp_OCISumProvenanceWarnsOnAMovableTag(t *testing.T) {
	c := qt.New(t)
	references := pushProvenanceReferences(c)

	out := applyProvenanceReference(c, references.tag)

	c.Assert(out, qt.Contains, "Warning: "+references.tag+" is a movable tag: ptah.sum travels inside the artifact")
	c.Assert(out, qt.Contains, "This tag resolved to "+references.resolved)
	c.Assert(out, qt.Contains, "pass "+references.digest+" to pin these exact bytes.")
}

// TestMigrateUp_OCISumProvenanceStaysSilentOnADigestPin is the control for the
// warning above: the same artifact named by the digest already pins the bytes,
// so there is nothing left to qualify. Without this half a run that warned on
// every reference would satisfy the test above.
func TestMigrateUp_OCISumProvenanceStaysSilentOnADigestPin(t *testing.T) {
	c := qt.New(t)
	references := pushProvenanceReferences(c)

	out := applyProvenanceReference(c, references.digest)

	c.Assert(out, qt.Not(qt.Contains), "movable tag")
	c.Assert(out, qt.Not(qt.Contains), "Warning:")
}
