package migrateup

// White-box testing required: mutableTagSumWarning is the predicate that
// decides whether a sum verification gets its provenance qualifier
// (stokaro/ptah#944), and the distinction it draws — a sum reviewed in version
// control beside the migrations versus a sum shipped inside an artifact a
// movable tag selected — is not observable through any exported API.

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

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	provenanceDigest      = "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	provenanceOtherDigest = "sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func ociTagSource(reference, tag, resolved string) migrationsource.Source {
	return migrationsource.Source{
		Display:   reference,
		DirFormat: migrator.MigrationDirFormatPtah,
		OCI: &migrationsource.OCI{
			Reference:       reference,
			Descriptor:      ocispec.Descriptor{Digest: digest.Digest(resolved)},
			Tag:             tag,
			DigestReference: "oci://reg.test/ptah/app@" + resolved,
		},
	}
}

func ociDigestSource(resolved string) migrationsource.Source {
	reference := "oci://reg.test/ptah/app@" + resolved
	return migrationsource.Source{
		Display:   reference,
		DirFormat: migrator.MigrationDirFormatPtah,
		OCI: &migrationsource.OCI{
			Reference:       reference,
			Descriptor:      ocispec.Descriptor{Digest: digest.Digest(resolved)},
			PinnedByDigest:  true,
			DigestReference: reference,
		},
	}
}

func localSource() migrationsource.Source {
	return migrationsource.Source{
		Display:   "/srv/app/migrations",
		DirFormat: migrator.MigrationDirFormatPtah,
	}
}

// TestMutableTagSumWarning pins which provenances get the qualifier. Only the
// tag rows may produce text: a digest reference already names the exact bytes,
// a local directory carries a sum reviewed beside the migrations, and an
// unhashed source verified nothing, so it claimed nothing to qualify.
func TestMutableTagSumWarning(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name            string
		source          func() migrationsource.Source
		verifiedSumFile string
		want            string
	}{
		{
			name: "oci tag verified names tag digest and pin",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceDigest)
			},
			verifiedSumFile: "ptah.sum",
			want: "oci://reg.test/ptah/app:release is a movable tag: ptah.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceDigest + " to pin these exact bytes.",
		},
		{
			name:            "oci digest verified stays silent",
			source:          func() migrationsource.Source { return ociDigestSource(provenanceDigest) },
			verifiedSumFile: "ptah.sum",
			want:            "",
		},
		{
			name:            "local directory verified stays silent",
			source:          localSource,
			verifiedSumFile: "ptah.sum",
			want:            "",
		},
		{
			name: "oci tag with nothing verified stays silent",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceDigest)
			},
			verifiedSumFile: "",
			want:            "",
		},
		{
			name: "oci tag quotes the digest it actually resolved to",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:release", "release", provenanceOtherDigest)
			},
			verifiedSumFile: "ptah.sum",
			want: "oci://reg.test/ptah/app:release is a movable tag: ptah.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceOtherDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceOtherDigest + " to pin these exact bytes.",
		},
		{
			name: "oci tag names the sum file that actually verified",
			source: func() migrationsource.Source {
				return ociTagSource("oci://reg.test/ptah/app:stable", "stable", provenanceDigest)
			},
			verifiedSumFile: "atlas.sum",
			want: "oci://reg.test/ptah/app:stable is a movable tag: atlas.sum travels inside the artifact, " +
				"so verifying it proves the pulled files are internally consistent, not that they are the " +
				"reviewed ones. This tag resolved to " + provenanceDigest + "; pass oci://reg.test/ptah/app@" +
				provenanceDigest + " to pin these exact bytes.",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(mutableTagSumWarning(tt.source(), tt.verifiedSumFile), qt.Equals, tt.want)
		})
	}
}

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

// TestMigrateUp_OCISumProvenance runs the real apply path against an
// in-process registry. It is the wiring half of stokaro/ptah#944: measurement
// showed `ptah.sum verified: migrations directory is intact` printed
// byte-identically for a tag whose content had been swapped and for the digest
// that pinned the reviewed bytes, so the tag row must gain provenance and the
// digest row must not.
func TestMigrateUp_OCISumProvenance(t *testing.T) {
	c := qt.New(t)
	const repository = "ptah/provenance"
	store := newOCIMemoryStore()
	host := startOCITestRegistry(c, store, repository)
	resolved := pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "widgets"), "release")
	tagReference := "oci://" + host + "/" + repository + ":release"
	digestReference := "oci://" + host + "/" + repository + "@" + resolved

	tests := []struct {
		name      string
		reference func() string
		verify    func(c *qt.C, out string)
	}{
		{
			name:      "movable tag carries provenance",
			reference: func() string { return tagReference },
			verify: func(c *qt.C, out string) {
				c.Assert(out, qt.Contains, "Warning: "+tagReference+" is a movable tag: ptah.sum travels inside the artifact")
				c.Assert(out, qt.Contains, "This tag resolved to "+resolved)
				c.Assert(out, qt.Contains, "pass oci://"+host+"/"+repository+"@"+resolved+" to pin these exact bytes.")
			},
		},
		{
			name:      "digest pin stays silent",
			reference: func() string { return digestReference },
			verify: func(c *qt.C, out string) {
				c.Assert(out, qt.Not(qt.Contains), "movable tag")
				c.Assert(out, qt.Not(qt.Contains), "Warning:")
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dbPath := filepath.Join(c.TempDir(), "provenance.db")

			out, err := runUpInternal(
				"--db-url", "sqlite://"+dbPath,
				"--migrations-dir", tt.reference(),
				"--verify-sum",
				"--plain-http",
				"--skip-report",
				"--verbose",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			// The warning qualifies the claim; it never replaces it and never
			// changes the exit status.
			c.Assert(out, qt.Contains, "ptah.sum verified: migrations directory is intact")
			c.Assert(out, qt.Contains, "Database is now at version: 1")
			tt.verify(c, out)
		})
	}
}
