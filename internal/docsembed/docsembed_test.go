package docsembed_test

import (
	"io/fs"
	"path"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/docs"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/docsembed"
)

// A path a result carries has to be one a person can open. The embedded
// filesystem is rooted at `docs/`, so a name read straight out of it is missing
// the one segment that would let anyone find the file.
func TestEveryDocumentIsNamedByItsRepositoryPath(t *testing.T) {
	c := qt.New(t)

	documents := docsembed.Documents()

	c.Assert(len(documents) > 0, qt.IsTrue)
	for _, document := range documents {
		c.Assert(strings.HasPrefix(document.Path, "docs/"), qt.IsTrue)
		assertEmbedded(c, strings.TrimPrefix(document.Path, "docs/"))
	}
}

func assertEmbedded(c *qt.C, name string) {
	_, err := fs.Stat(docs.FS, name)
	c.Assert(err, qt.IsNil)
}

func TestEveryDocumentCarriesItsText(t *testing.T) {
	c := qt.New(t)

	for _, document := range docsembed.Documents() {
		c.Assert(document.Content, qt.Not(qt.Equals), "")
	}
}

// Indexing costs a few milliseconds, and every `ptah` invocation that is not
// `ptah mcp` never asks a documentation question. The index is built once, on
// first use.
func TestIndexIsBuiltOnceAndShared(t *testing.T) {
	c := qt.New(t)

	c.Assert(docsembed.Index(), qt.Equals, docsembed.Index())
}

func TestIndexCoversEveryEmbeddedDocument(t *testing.T) {
	c := qt.New(t)

	index := docsembed.Index()

	c.Assert(index.DocumentCount(), qt.Equals, len(docsembed.Documents()))
	c.Assert(index.PassageCount() > index.DocumentCount(), qt.IsTrue)
}

// The index answers from the embedded documentation and names where each answer
// came from.
func TestSearchAnswersFromTheEmbeddedDocumentation(t *testing.T) {
	c := qt.New(t)

	results := docsembed.Index().Search("migration", 5)

	c.Assert(len(results) > 0, qt.IsTrue)
	for _, result := range results {
		c.Assert(strings.HasPrefix(result.Path, "docs/"), qt.IsTrue)
		assertEmbedded(c, strings.TrimPrefix(result.Path, "docs/"))
	}
}

// Every markdown file the binary carries is a document the index can answer
// from. Counting documents against the index measures nothing on its own: a
// filter that drops a whole extension drops it from both sides at once, and the
// two counts still agree.
func TestDocumentsCoverEveryEmbeddedFile(t *testing.T) {
	c := qt.New(t)

	read := make(map[string]bool, len(docsembed.Documents()))
	for _, document := range docsembed.Documents() {
		read[strings.TrimPrefix(document.Path, "docs/")] = true
	}

	c.Assert(read, qt.DeepEquals, embeddedMarkdown(c))
}

func embeddedMarkdown(c *qt.C) map[string]bool {
	names := make(map[string]bool)
	err := fs.WalkDir(docs.FS, ".", func(name string, entry fs.DirEntry, err error) error {
		c.Assert(err, qt.IsNil)
		if entry.IsDir() {
			return nil
		}
		names = markIfMarkdown(names, name)
		return nil
	})
	c.Assert(err, qt.IsNil)
	// The corpus has both extensions, so a filter that reads only one of them
	// is a filter this comparison can see.
	c.Assert(countExt(names, ".md") > 0, qt.IsTrue)
	c.Assert(countExt(names, ".mdx") > 0, qt.IsTrue)
	return names
}

func markIfMarkdown(names map[string]bool, name string) map[string]bool {
	ext := strings.ToLower(path.Ext(name))
	if ext != ".md" && ext != ".mdx" {
		return names
	}
	names[name] = true
	return names
}

func countExt(names map[string]bool, ext string) int {
	found := 0
	for name := range names {
		found += extHit(name, ext)
	}
	return found
}

func extHit(name, ext string) int {
	if strings.EqualFold(path.Ext(name), ext) {
		return 1
	}
	return 0
}

// The documentation's version is the build's version, because the documentation
// is compiled from the same tree as the code. There is no second version that
// could disagree with it -- which is the point, not a simplification.
func TestVersionNamesTheBuild(t *testing.T) {
	c := qt.New(t)

	documentation := docsembed.Version()

	c.Assert(documentation.Version, qt.Equals, buildinfo.Resolve().Version)
	c.Assert(documentation.Commit, qt.Equals, buildinfo.Resolve().Commit)
	c.Assert(documentation.Version, qt.Not(qt.Equals), "")
	c.Assert(documentation.Commit, qt.Not(qt.Equals), "")
}
