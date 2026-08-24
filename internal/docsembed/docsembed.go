// Package docsembed turns the documentation carried in the binary into a
// searchable index.
//
// It is the seam between where the documentation is kept and how it is served:
// [go.5x5.cz/ptah/docs] holds the files, [docsindex] knows how to search them,
// and neither has to know about the other.
package docsembed

import (
	"io/fs"
	"path"
	"strings"
	"sync"

	"go.5x5.cz/ptah/docs"
	"go.5x5.cz/ptah/internal/docsindex"
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
