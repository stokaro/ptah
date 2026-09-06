// Package docsembed turns the documentation carried in the binary into a
// searchable index.
//
// It is the seam between where the documentation is kept and how it is served:
// [ptah.run/docs] holds the files, [docsindex] knows how to search them,
// and neither has to know about the other.
package docsembed

import (
	"io/fs"
	"path"
	"strings"
	"sync"

	"ptah.run/docs"
	"ptah.run/internal/buildinfo"
	"ptah.run/internal/docsindex"
)

// docsPrefix is what an embedded path is reported as. The embedded filesystem
// is rooted at the repository's `docs/` directory, so a path read out of it is
// missing the one segment that would let a person open the file the answer came
// from.
const docsPrefix = "docs/"

var (
	once   sync.Once
	shared *docsindex.Index
)

// Version names the documentation this binary carries.
//
// It is the build's own version, and that is not a shortcut: the documentation
// is compiled from the same tree as the code, so there is no pair of versions
// that could disagree. The issue asked what happens when the documentation and
// the binary describe different things (stokaro/ptah#2123) -- embedding the
// source at build time is the answer that makes the question unanswerable,
// and reporting the build is how a reader can tell which documentation they
// got.
//
// A commit rather than a tag alone, because "dev" is what an unstamped build
// reports and it names nothing on its own.
func Version() Documentation {
	info := buildinfo.Resolve()
	return Documentation{Version: info.Version, Commit: info.Commit}
}

// Documentation names one build's documentation.
type Documentation struct {
	// Version is the build's version, "dev" when nothing stamped it.
	Version string `json:"version"`
	// Commit is the revision the documentation was compiled from, "unknown"
	// when the build carries no VCS information.
	Commit string `json:"commit"`
}

// Index returns the index over Ptah's own documentation, building it on first
// use.
//
// Indexing 110 documents costs a few milliseconds, which is not worth paying in
// a process that never answers a documentation question -- and every `ptah`
// invocation that is not `ptah mcp` is such a process.
func Index() *docsindex.Index {
	once.Do(func() {
		shared = docsindex.Build(Documents())
	})
	return shared
}

// Documents reads every embedded document, with its repository-relative path.
func Documents() []docsindex.Document {
	var documents []docsindex.Document
	_ = fs.WalkDir(docs.FS, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !isMarkdown(name) {
			return nil
		}
		content, readErr := fs.ReadFile(docs.FS, name)
		if readErr != nil {
			return nil
		}
		documents = append(documents, docsindex.Document{
			Path:    docsPrefix + name,
			Content: string(content),
		})
		return nil
	})
	return documents
}

func isMarkdown(name string) bool {
	ext := strings.ToLower(path.Ext(name))
	return ext == ".md" || ext == ".mdx"
}
