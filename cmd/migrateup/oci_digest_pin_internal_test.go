package migrateup

// White-box testing required: these tests reuse the in-process OCI registry
// harness this package already builds for stokaro/ptah#944 (ociMemoryStore,
// startOCITestRegistry, pushProvenanceArtifact, writeHashedProvenanceDir,
// runUpInternal). No exported seam lets a test repoint a registry tag between
// two resolutions of the same reference, and no exported seam lets a test serve
// one manifest under another manifest's digest, which is the registry behavior
// a digest pin exists to defend against (stokaro/ptah#1094).

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrateUp_OCITagAndDigestResolvesByDigest is the fixture stokaro/ptah#1094
// asks for. Both artifacts are pushed to the same :release tag, so the tag is
// repointed after the digest was captured and the two candidate behaviors --
// honor the digest, or quietly follow the tag -- create differently named
// tables. The bare-tag row is the control: without it the digest rows would
// pass even in a build that ignored the digest, because a tag that never moved
// resolves to the pinned bytes anyway.
func TestMigrateUp_OCITagAndDigestResolvesByDigest(t *testing.T) {
	c := qt.New(t)
	const repository = "ptah/pinned"
	store := newOCIMemoryStore()
	host := startOCITestRegistry(c, store, repository)
	reviewed := pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "reviewed"), "release")
	swapped := pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "swapped"), "release")
	c.Assert(swapped, qt.Not(qt.Equals), reviewed,
		qt.Commentf("the two artifacts must differ, or no row can tell the tag from the digest"))
	base := "oci://" + host + "/" + repository

	tests := []struct {
		name        string
		reference   string
		wantTable   string
		absentTable string
		verify      func(c *qt.C, out string)
	}{
		{
			name:        "bare tag follows the repoint",
			reference:   base + ":release",
			wantTable:   "swapped",
			absentTable: "reviewed",
			verify: func(c *qt.C, out string) {
				c.Assert(out, qt.Contains, "is a movable tag")
			},
		},
		{
			name:        "tag and digest resolves by the digest",
			reference:   base + ":release@" + reviewed,
			wantTable:   "reviewed",
			absentTable: "swapped",
			verify: func(c *qt.C, out string) {
				// #1094 definition of done: the readable pin counts as a digest
				// reference for the #1093 warning, because a digest was pinned.
				c.Assert(out, qt.Not(qt.Contains), "movable tag")
				c.Assert(out, qt.Not(qt.Contains), "Warning:")
			},
		},
		{
			name:        "bare digest resolves the same bytes",
			reference:   base + "@" + reviewed,
			wantTable:   "reviewed",
			absentTable: "swapped",
			verify: func(c *qt.C, out string) {
				c.Assert(out, qt.Not(qt.Contains), "Warning:")
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dbURL := "sqlite://" + filepath.Join(c.TempDir(), "pinned.db")

			out, err := runUpInternal(
				"--db-url", dbURL,
				"--migrations-dir", tt.reference,
				"--verify-sum",
				"--plain-http",
				"--skip-report",
				"--verbose",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "ptah.sum verified: migrations directory is intact")
			c.Assert(sqliteMigrateUpTableExists(c, dbURL, tt.wantTable), qt.IsTrue,
				qt.Commentf("%s", out))
			c.Assert(sqliteMigrateUpTableExists(c, dbURL, tt.absentTable), qt.IsFalse,
				qt.Commentf("%s", out))
			tt.verify(c, out)
		})
	}
}

// TestMigrateUp_OCIDigestNotInRegistryIsRefused pins that a digest this registry
// never held fails instead of falling back to the tag written beside it. A
// fallback would make the pin decoration: the reference would run whatever
// :release points at today, which is the outcome #1093 warns about and #1094
// asks operators to escape.
func TestMigrateUp_OCIDigestNotInRegistryIsRefused(t *testing.T) {
	c := qt.New(t)
	const repository = "ptah/unserved"
	store := newOCIMemoryStore()
	host := startOCITestRegistry(c, store, repository)
	pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "swapped"), "release")
	// Minted in a registry of its own: a well-formed artifact digest that the
	// registry under test has never served.
	elsewhere := pushProvenanceArtifact(c, newOCIMemoryStore(), writeHashedProvenanceDir(c, "reviewed"), "release")
	dbURL := "sqlite://" + filepath.Join(c.TempDir(), "unserved.db")

	out, err := runUpInternal(
		"--db-url", dbURL,
		"--migrations-dir", "oci://"+host+"/"+repository+":release@"+elsewhere,
		"--verify-sum",
		"--plain-http",
		"--skip-report",
		"--verbose",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, elsewhere,
		qt.Commentf("the refusal must name the digest that was asked for"))
	c.Assert(err.Error(), qt.Contains, "not found")
	c.Assert(sqliteMigrateUpTableExists(c, dbURL, "swapped"), qt.IsFalse,
		qt.Commentf("the tag must not stand in for the digest: %s", out))
	c.Assert(sqliteMigrateUpTableExists(c, dbURL, "reviewed"), qt.IsFalse)
}

// TestMigrateUp_OCISubstitutedManifestIsRefused answers the question a parse
// test cannot: is the digest enforced, or merely carried? This registry answers
// every manifest request with the bytes :release resolves to, whatever digest
// the client asked for -- a compromised or simply wrong registry. Both rows must
// refuse before any DDL runs; the second row also lies in the
// Docker-Content-Digest header, so only hashing the body catches it.
func TestMigrateUp_OCISubstitutedManifestIsRefused(t *testing.T) {
	const repository = "ptah/substituted"

	tests := []struct {
		name   string
		header func(served, requested string) string
	}{
		{
			name:   "registry advertises the bytes it actually served",
			header: func(served, _ string) string { return served },
		},
		{
			name:   "registry echoes the digest that was asked for",
			header: func(_, requested string) string { return requested },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			store := newOCIMemoryStore()
			host := startSubstitutingOCITestRegistry(c, store, repository, tt.header)
			pushProvenanceArtifact(c, store, writeHashedProvenanceDir(c, "swapped"), "release")
			reviewed := pushProvenanceArtifact(
				c,
				newOCIMemoryStore(),
				writeHashedProvenanceDir(c, "reviewed"),
				"release",
			)
			dbURL := "sqlite://" + filepath.Join(c.TempDir(), "substituted.db")

			out, err := runUpInternal(
				"--db-url", dbURL,
				"--migrations-dir", "oci://"+host+"/"+repository+":release@"+reviewed,
				"--verify-sum",
				"--plain-http",
				"--skip-report",
				"--verbose",
			)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Contains, "mismatch",
				qt.Commentf("the refusal must be a digest mismatch, not an unrelated failure"))
			c.Assert(sqliteMigrateUpTableExists(c, dbURL, "swapped"), qt.IsFalse,
				qt.Commentf("substituted bytes must never reach the database: %s", out))
			c.Assert(sqliteMigrateUpTableExists(c, dbURL, "reviewed"), qt.IsFalse)
		})
	}
}

// serveSubstitutedManifest answers every manifest request with the bytes tag
// currently resolves to, ignoring the digest in the request path. header picks
// the digest the response advertises, so a registry that lies only in the body
// and a registry that also lies in the header are both measurable.
func (s *ociMemoryStore) serveSubstitutedManifest(
	tag string,
	header func(served, requested string) string,
) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		descriptor, err := s.Resolve(request.Context(), tag)
		if err != nil {
			http.Error(writer, "manifest unknown", http.StatusNotFound)
			return
		}
		s.mu.Lock()
		blob := s.blobs[descriptor.Digest]
		s.mu.Unlock()
		writer.Header().Set("Content-Type", descriptor.MediaType)
		writer.Header().Set(
			"Docker-Content-Digest",
			header(descriptor.Digest.String(), request.PathValue("reference")),
		)
		writer.Header().Set("Content-Length", strconv.Itoa(len(blob.data)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(blob.data)
	}
}

// startSubstitutingOCITestRegistry serves store over the read half of the OCI
// distribution API with the substituting manifest handler in place. Blobs are
// served honestly, so a refusal can only come from the manifest the pin names.
func startSubstitutingOCITestRegistry(
	c *qt.C,
	store *ociMemoryStore,
	repository string,
	header func(served, requested string) string,
) string {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc(
		"/v2/"+repository+"/manifests/{reference}",
		store.serveSubstitutedManifest("release", header),
	)
	mux.HandleFunc("/v2/"+repository+"/blobs/{digest}", store.serveBlob)
	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}
