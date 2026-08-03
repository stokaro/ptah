package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const compatHashDigest = "sha256:" +
	"c1eb0d4d1a1d6a48d5f3c6f2b7a9e0c4d8b2f6a1e3c5079b4d8e2a6c0f4b8d2e"

// TestCompatMigrateHash_RefusesOCIDirAtTheURLGate guards the boundary
// stokaro/ptah#1094 created. `atlas migrate hash` reuses the native
// `ptah migrations hash` command, and that command now refuses an oci://
// directory with a sentence about immutable artifacts. On the compatibility
// surface the refusal must still come from the file:// URL gate instead, at
// exit 1, because that is where every other compat verb refuses a remote
// directory and it is the outcome the pinned community binary produces
// (`unsupported driver "oci"`, exit 1, measured 2026-08-03 through
// ptah-atlas-conformance/bin/atlas).
//
// Discriminator: if the native message ever reached this surface, the
// qt.Not(qt.Contains) row below goes red rather than silently exporting native
// wording into the compat namespace.
func TestCompatMigrateHash_RefusesOCIDirAtTheURLGate(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		dir  string
	}{
		{name: "tag and digest", dir: "oci://registry.example:5000/team/app:release@" + compatHashDigest},
		{name: "bare digest", dir: "oci://registry.example:5000/team/app@" + compatHashDigest},
		{name: "tag", dir: "oci://registry.example:5000/team/app:release"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, _, err := runCompat("migrate", "hash", "--dir", tt.dir)

			c.Assert(err, qt.ErrorMatches,
				`atlas migrate hash --dir: only local file:// migration directories are supported`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "an OCI artifact is immutable")
		})
	}
}

// TestCompatMigrateHash_LocalDirStillHashes is the non-interference control for
// the rows above: a compat surface that refused every --dir would satisfy them
// all. This one has to keep working, and it is the same call the rest of the
// compat suite builds its fixtures with.
func TestCompatMigrateHash_LocalDirStillHashes(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)

	c.Assert(err, qt.IsNil)
	_, statErr := os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)
}
