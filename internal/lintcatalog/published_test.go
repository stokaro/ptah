package lintcatalog_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
)

// The page documents the Spanner and YDB rules, which no bundled owner
// serves: the catalog reads them from the official providers' records. The
// control shows the records are what restore them: the bundled runtime alone
// documents none of them, and the catalog refuses it for losing rules it
// describes, Spanner's first by name.
func TestEntries_TheRecordsKeepTheProviderRules(t *testing.T) {
	c := qt.New(t)
	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	engines := make(map[string][]string)
	for _, entry := range slices.DeleteFunc(slices.Clone(entries), func(entry lintcatalog.Entry) bool {
		return entry.Owner != lintcatalog.OwnerProvider
	}) {
		engines[entry.Dialects[0]] = append(engines[entry.Dialects[0]], entry.Name)
	}

	_, withoutRecord := lintcatalog.EntriesFrom(builtintest.Runtime())

	c.Assert(engines["spanner"], qt.DeepEquals, []string{"side-effect/drop-row-deletion-policy"})
	c.Assert(engines["ydb"], qt.Not(qt.HasLen), 0)
	c.Assert(withoutRecord, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) .*side-effect/drop-row-deletion-policy.* that no longer exist: .*`)
}
