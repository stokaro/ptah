package docs_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/docs"
)

// What the binary carries is what the repository holds.
//
// This is the whole reason the markdown is embedded as written rather than as a
// prebuilt index: drift between the two is not something to detect, it is
// something that cannot be expressed. The one way it could still happen is a
// documentation directory the embed patterns do not name -- `go:embed` is
// silent about files it was never asked for -- so the patterns are measured
// against the tree rather than trusted.
//
// This test runs in the `docs` package because that is where the files are: an
// on-disk walk from here needs no path that leaves the package, and a test that
// reaches out of its own directory to find the repository is one that breaks
// the moment it is run from somewhere else.
func TestEmbeddedSetMatchesTheRepository(t *testing.T) {
	c := qt.New(t)

	c.Assert(embeddedPaths(c), qt.DeepEquals, onDiskPaths(c))
}

func TestEmbeddedSetCoversEachDocumentationArea(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{name: "guides at the top of docs", prefix: "capabilities.md"},
		{name: "decision records", prefix: "adr/"},
		{name: "site pages", prefix: "site/src/content/docs/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(countWithPrefix(embeddedPaths(c), tt.prefix) > 0, qt.IsTrue)
		})
	}
}

// The site's own README and content inventory describe how the site is built,
// which is not a question anyone asks Ptah about its own behavior.
func TestSiteToolingIsNotEmbedded(t *testing.T) {
	c := qt.New(t)

	embedded := embeddedPaths(c)

	c.Assert(countWithPrefix(embedded, "site/scripts/"), qt.Equals, 0)
	c.Assert(countWithPrefix(embedded, "site/README"), qt.Equals, 0)
	c.Assert(countWithPrefix(embedded, "site/CONTENT_INVENTORY"), qt.Equals, 0)
}

// The feature inventory is a contributor register, not product documentation.
//
// It sits in this directory because that is where a reader looks for it, and it
// is `.json` because the patterns above name `*.md`, `adr/*.md` and the site
// content: a Markdown file here would ship inside every binary and answer
// `search_docs`, which is what happened to the attempt this replaces.
//
// Asserted against docs.FS DIRECTLY rather than through embeddedPaths. That
// helper filters to Markdown, so a check routed through it would reduce to
// `0 == 0` and stay green if the embed ever grew to include JSON -- the
// tautology-on-the-half-you-are-not-looking-at hazard AGENTS.md names, which
// applies here even though nothing about it is platform-specific.
func TestFeatureInventoryIsNotEmbedded(t *testing.T) {
	c := qt.New(t)

	_, err := fs.Stat(docs.FS, "feature-inventory.json")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)

	// The control: the file this asserts the absence of has to exist on disk,
	// or the assertion above passes because nothing was ever generated.
	_, diskErr := os.Stat("feature-inventory.json")
	c.Assert(diskErr, qt.IsNil)
}

func embeddedPaths(c *qt.C) []string {
	var paths []string
	err := fs.WalkDir(docs.FS, ".", func(name string, entry fs.DirEntry, err error) error {
		c.Assert(err, qt.IsNil)
		if entry.IsDir() {
			return nil
		}
		paths = appendMarkdown(paths, name)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return paths
}

// onDiskPaths is the policy this package's patterns are meant to express: every
// markdown file at the top of `docs/`, every decision record, and the pages the
// site is built from -- and nothing else under `docs/site`.
func onDiskPaths(c *qt.C) []string {
	var paths []string
	err := filepath.WalkDir(".", func(name string, entry os.DirEntry, err error) error {
		c.Assert(err, qt.IsNil)
		name = filepath.ToSlash(name)
		if entry.IsDir() {
			return skipDirIfUnreachable(name)
		}
		paths = appendMarkdownFromDocumentationDir(paths, name)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return paths
}

// skipDirIfUnreachable prunes the walk to the directories that hold
// documentation or lead to it. Descending into `docs/site/node_modules` costs
// more than the rest of the walk put together.
func skipDirIfUnreachable(name string) error {
	reachable := name == "." || name == "adr" ||
		name == "site" || name == "site/src" || name == "site/src/content" ||
		name == "site/src/content/docs" || strings.HasPrefix(name, "site/src/content/docs/")
	if reachable {
		return nil
	}
	return filepath.SkipDir
}

// appendMarkdownFromDocumentationDir collects a file only from a directory that
// holds documentation. `site` and `site/src` are walked to reach the content
// below them; markdown sitting directly in them is site tooling.
func appendMarkdownFromDocumentationDir(paths []string, name string) []string {
	dir := path.Dir(name)
	documentation := dir == "." || dir == "adr" ||
		dir == "site/src/content/docs" || strings.HasPrefix(dir, "site/src/content/docs/")
	if !documentation {
		return paths
	}
	return appendMarkdown(paths, name)
}

func appendMarkdown(paths []string, name string) []string {
	ext := strings.ToLower(path.Ext(name))
	if ext != ".md" && ext != ".mdx" {
		return paths
	}
	return append(paths, name)
}

func countWithPrefix(paths []string, prefix string) int {
	found := 0
	for _, p := range paths {
		found += prefixHit(p, prefix)
	}
	return found
}

func prefixHit(name, prefix string) int {
	if strings.HasPrefix(name, prefix) {
		return 1
	}
	return 0
}
