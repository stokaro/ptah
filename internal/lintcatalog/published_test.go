package lintcatalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
)

// The page documents the Spanner and YDB rules, which no bundled owner
// serves: the catalog reads them from the official providers' records. The
// control shows the records are what restore them: the bundled runtime alone
// documents none of them, and the catalog refuses it for losing rules it
// describes, Spanner's first by code.
func TestEntries_TheRecordsKeepTheProviderRules(t *testing.T) {
	c := qt.New(t)
	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	families := make(map[string][]string)
	for _, entry := range entries {
		families[entry.Code[:2]] = append(families[entry.Code[:2]], entry.Code)
	}

	_, withoutRecord := lintcatalog.EntriesFrom(builtintest.Runtime())

	c.Assert(families["SP"], qt.DeepEquals, []string{"SP101", "SP102"})
	c.Assert(families["YD"], qt.Not(qt.HasLen), 0)
	c.Assert(withoutRecord, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) SP101, SP102, YD.* that no longer exist: .*`)
}
