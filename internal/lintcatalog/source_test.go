package lintcatalog_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
)

// TestCatalogFromARuntime_HappyPath hands the catalog the bundled runtime
// explicitly and gets the catalog the documentation renders.
func TestCatalogFromARuntime_HappyPath(t *testing.T) {
	c := qt.New(t)
	want := must.Must(lintcatalog.Entries())
	var page bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdown(&page), qt.IsNil)

	got, err := lintcatalog.EntriesFrom(builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, want)
	var from bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdownFrom(&from, builtintest.Runtime()), qt.IsNil)
	c.Assert(from.String(), qt.Equals, page.String())
}

// TestCatalogFromARuntime_FailurePath reads the rules from a runtime whose
// owners publish none. The meanings declared for the owners' rules then
// describe nothing, and the join names them rather than rendering a catalog
// that lost a family: the runtime is where those rules come from.
func TestCatalogFromARuntime_FailurePath(t *testing.T) {
	c := qt.New(t)
	empty := must.Must(engine.New())

	entries, err := lintcatalog.MigrationEntriesFrom(empty)
	c.Assert(err, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) .*\bPG101\b.*\bYD101\b.* that no longer exist: .*`)
	c.Assert(entries, qt.IsNil)
	all, err := lintcatalog.EntriesFrom(empty)
	c.Assert(err, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) .* that no longer exist: .*`)
	c.Assert(all, qt.IsNil)
	var page bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdownFrom(&page, empty), qt.ErrorMatches, `the catalog documents migration lint rule\(s\) .* that no longer exist: .*`)
	c.Assert(page.String(), qt.Equals, "")
}
