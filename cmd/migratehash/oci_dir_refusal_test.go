package migratehash_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/migratesum"
)

const refusalDigest = "sha256:" +
	"c1eb0d4d1a1d6a48d5f3c6f2b7a9e0c4d8b2f6a1e3c5079b4d8e2a6c0f4b8d2e"

// TestHash_RefusesOCIDirectoryByName pins the fourth verb stokaro/ptah#1094
// names. up, status and down resolve oci://registry/repository:tag@sha256:D by
// its digest; hash cannot, because it writes the integrity file back into the
// directory it hashed. Before this change every row below failed as
// "stat oci://...: no such file or directory", which names a file that was
// never a path and reads as "oci:// is not a thing here" -- the opposite of
// what the other verbs do. The refusal must say the artifact is immutable and
// point at the workflow that does work.
func TestHash_RefusesOCIDirectoryByName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		dir  string
	}{
		{
			name: "tag and digest",
			dir:  "oci://registry.example:5000/team/app:release@" + refusalDigest,
		},
		{
			name: "bare digest",
			dir:  "oci://registry.example:5000/team/app@" + refusalDigest,
		},
		{
			name: "tag",
			dir:  "oci://registry.example:5000/team/app:release",
		},
		{
			name: "no selector",
			dir:  "oci://registry.example:5000/team/app",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			stdout, err := execute("--dir", tt.dir)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "cannot hash "+tt.dir)
			c.Assert(err.Error(), qt.Contains, "an OCI artifact is immutable")
			c.Assert(err.Error(), qt.Contains, "ptah migrations push")
			c.Assert(err.Error(), qt.Not(qt.Contains), "no such file or directory")
			// Unchanged from the stat failure this replaces: the refusal is a
			// clearer sentence, not a new outcome.
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stdout, qt.Not(qt.Contains), "hashed")
		})
	}
}

// TestHash_OCIRefusalLeavesLocalDirectoriesAlone is the non-interference
// control. A guard that fired on every directory would still make every row of
// the test above pass, so the refusal has to be measured against a path that
// must still be hashed -- including one whose name merely contains "oci".
func TestHash_OCIRefusalLeavesLocalDirectoriesAlone(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		base string
	}{
		{name: "ordinary directory", base: "migrations"},
		{name: "directory named after the scheme", base: "oci"},
		{name: "directory whose name embeds the scheme", base: "oci--registry.example"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := filepath.Join(c.TempDir(), tt.base)
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
				[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)

			stdout, err := execute("--dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
			c.Assert(stdout, qt.Contains, "1 migration file(s) hashed")
			c.Assert(strings.Contains(stdout, "immutable"), qt.IsFalse)
			result, err := migratesum.VerifyDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(result.OK(), qt.IsTrue)
		})
	}
}
