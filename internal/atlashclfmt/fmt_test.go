package atlashclfmt_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlashclfmt"
)

func TestFormatPaths_HappyPathFormatsFilesRecursively(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	changed := filepath.Join(dir, "changed.hcl")
	nestedChanged := filepath.Join(dir, "nested", "changed.hcl")
	unchanged := filepath.Join(dir, "nested", "unchanged.hcl")
	ignored := filepath.Join(dir, "notes.txt")
	c.Assert(os.MkdirAll(filepath.Dir(unchanged), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(changed, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(nestedChanged, []byte(`schema "nested"{}`+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(unchanged, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(ignored, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)

	result, err := atlashclfmt.FormatPaths([]string{dir})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []string{changed, nestedChanged})
	c.Assert(readFile(c, changed), qt.Equals, `schema "main" {}
`)
	c.Assert(readFile(c, nestedChanged), qt.Equals, `schema "nested" {}
`)
	c.Assert(readFile(c, unchanged), qt.Equals, `schema "main" {}
`)
	c.Assert(readFile(c, ignored), qt.Equals, `schema "main"{}`+"\n")
}

func TestFormatFile_HappyPathIgnoresNonHCLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "notes.txt")
	c.Assert(os.WriteFile(path, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)

	changed, err := atlashclfmt.FormatFile(path)

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(readFile(c, path), qt.Equals, `schema "main"{}`+"\n")
}

func TestFormatFile_FailurePathRejectsInvalidHCLWithoutRewriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.hcl")
	original := []byte(`schema "main" {
`)
	c.Assert(os.WriteFile(path, original, 0o600), qt.IsNil)

	changed, err := atlashclfmt.FormatFile(path)

	c.Assert(err, qt.ErrorMatches, `schema fmt .*bad\.hcl: .*`)
	c.Assert(changed, qt.IsFalse)
	c.Assert(readFileBytes(c, path), qt.DeepEquals, original)
}

func TestFormatPath_FailurePathReportsMissingPath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "missing.hcl")

	result, err := atlashclfmt.FormatPath(path)

	c.Assert(err, qt.ErrorMatches, `schema fmt .*missing\.hcl: .*`)
	c.Assert(result, qt.HasLen, 0)
}

func readFile(c *qt.C, path string) string {
	c.Helper()
	return string(readFileBytes(c, path))
}

func readFileBytes(c *qt.C, path string) []byte {
	c.Helper()
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return data
}
