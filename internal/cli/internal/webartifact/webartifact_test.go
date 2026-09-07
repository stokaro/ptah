package webartifact_test

import (
	"context"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/cli/internal/webartifact"
	"ptah.run/internal/schemadoc"
)

// bookshop is a schema with a dependency, a foreign key and an index, so the
// artifact this writes is the one the issue asks to be verified.
func bookshop() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Author", Name: "authors"},
			{StructName: "Book", Name: "books"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Author", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Book", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Book", Name: "author_id", Type: "BIGINT", Foreign: "authors(id)"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Book", Name: "idx_books_author", Fields: []string{"author_id"}},
		},
	}
}

// The artifact is on disk and is the whole document. A run that reported a path
// to a file it did not finish writing would be worse than one that failed.
func TestWrite_LeavesTheWholeDocumentOnDisk(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")

	result, diagnostics, err := webartifact.Write(context.Background(), bookshop(), webartifact.Options{
		Title: "Bookshop",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics, qt.HasLen, 0)
	t.Cleanup(func() { _ = os.Remove(result.Path) })

	data, err := os.ReadFile(result.Path)
	c.Assert(err, qt.IsNil)
	page := string(data)
	c.Assert(strings.HasPrefix(page, "<!doctype html>"), qt.IsTrue)
	c.Assert(page, qt.Contains, "authors")
	c.Assert(page, qt.Contains, "books")
	c.Assert(page, qt.Contains, "idx_books_author")
	c.Assert(page, qt.Contains, `class="edge"`)
	c.Assert(strings.HasSuffix(strings.TrimSpace(page), "</html>"), qt.IsTrue)
}

// Nothing is fetched when the document is opened. This is the property the
// whole design follows from, asserted on the artifact a person actually
// receives rather than only on the renderer's output.
func TestWrite_ProducesADocumentThatFetchesNothing(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")

	result, _, err := webartifact.Write(context.Background(), bookshop(), webartifact.Options{})

	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = os.Remove(result.Path) })

	data, err := os.ReadFile(result.Path)
	c.Assert(err, qt.IsNil)
	page := string(data)
	for _, element := range []string{"<script", "<link", "<img", "@import", `src="http`} {
		c.Assert(page, qt.Not(qt.Contains), element)
	}
}

// A headless run reports a path and no browser, and says which. The artifact is
// the deliverable, so this is a success and not a degraded one.
func TestWrite_ReportsThePathWhenNothingOpened(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")

	result, _, err := webartifact.Write(context.Background(), bookshop(), webartifact.Options{})

	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = os.Remove(result.Path) })
	c.Assert(result.Opened, qt.IsFalse)
	c.Assert(result.Reason, qt.Equals, "CI is set")

	var out strings.Builder
	webartifact.Report(&out, result)
	c.Assert(out.String(), qt.Contains, "ERD written to "+result.Path)
	c.Assert(out.String(), qt.Contains, "not opened: CI is set")
}

// The marks reach the document. The mapping from a comparison to a mark is the
// caller's, so this pins only that a mark passed in is a mark drawn.
func TestWrite_CarriesTheComparisonMarks(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")

	result, _, err := webartifact.Write(context.Background(), bookshop(), webartifact.Options{
		Changes: map[string]schemadoc.ChangeKind{"books": schemadoc.ChangeAdded},
	})

	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = os.Remove(result.Path) })

	data, err := os.ReadFile(result.Path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `class="node chg-added"`)
}

// A report that opened nothing still prints the path, and one that opened
// something prints it too: the window may be on another desktop, and the path
// is what a reviewer attaches.
func TestReport_AlwaysNamesThePath(t *testing.T) {
	c := qt.New(t)

	var out strings.Builder
	webartifact.Report(&out, webartifact.Result{Path: "/tmp/erd.html", Opened: true})

	c.Assert(out.String(), qt.Equals, "ERD written to /tmp/erd.html\n")
}
