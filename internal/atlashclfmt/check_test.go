package atlashclfmt_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashclfmt"
)

func TestCheckPathsReportsUnformattedFilesWithoutRewriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	messy := filepath.Join(dir, "messy.hcl")
	clean := filepath.Join(dir, "clean.hcl")
	const messyContent = `schema   "main" {` + "\n}\n"
	c.Assert(os.WriteFile(messy, []byte(messyContent), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(clean, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)

	unformatted, err := atlashclfmt.CheckPaths([]string{dir})

	c.Assert(err, qt.IsNil)
	c.Assert(unformatted, qt.DeepEquals, []string{messy})
	c.Assert(readFile(c, messy), qt.Equals, messyContent)
	c.Assert(readFile(c, clean), qt.Equals, "schema \"main\" {\n}\n")
}

func TestCheckPathsPassesOnCanonicalFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "clean.hcl")
	c.Assert(os.WriteFile(path, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)

	unformatted, err := atlashclfmt.CheckPaths([]string{dir})

	c.Assert(err, qt.IsNil)
	c.Assert(unformatted, qt.HasLen, 0)
}
