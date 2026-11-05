package lintcatalog_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
)

// The page documents the YDB rules, which no bundled owner serves: the catalog
// reads them from the official provider's record. The control shows the YDB
// rules are what the record restores: the bundled runtime alone documents none
// of them, and the catalog refuses it for losing rules it describes.
func TestEntries_TheRecordKeepsTheYDBRules(t *testing.T) {
	c := qt.New(t)
	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	ydb := slices.DeleteFunc(slices.Clone(entries), func(entry lintcatalog.Entry) bool { return !strings.HasPrefix(entry.Code, "YD") })

	_, withoutRecord := lintcatalog.EntriesFrom(builtintest.Runtime())

	c.Assert(ydb, qt.Not(qt.HasLen), 0)
	c.Assert(withoutRecord, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) YD.* that no longer exist: .*`)
}
