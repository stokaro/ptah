package lintcatalog_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
	"ptah.run/internal/providerpublished"
)

// withoutYDB is the bundled runtime as it is once the bundle no longer
// carries the YDB engine: it resolves neither YDB spelling and publishes none
// of the YDB owner's rules.
type withoutYDB struct{ *engine.Runtime }

func (w withoutYDB) ResolveTarget(name string) (schemaext.TargetSelection, error) {
	if name == "ydb" || name == "ydbs" {
		return schemaext.TargetSelection{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, name)
	}
	return w.Runtime.ResolveTarget(name)
}

func (w withoutYDB) LintRules(target string) ([]lintext.Rule, error) {
	rules, err := w.Runtime.LintRules(target)
	return slices.DeleteFunc(rules, func(rule lintext.Rule) bool { return slices.Equal(rule.Targets, []string{"ydb"}) }), err
}

// The page keeps the YDB rules when the bundle stops carrying the engine: a
// runtime without the YDB owner, documented through the official provider's
// record, enumerates what the bundle enumerates today and renders the same
// page. The control shows the YDB rules are what the record restores.
func TestEntries_TheRecordKeepsTheYDBRulesWithoutTheBundledOwner(t *testing.T) {
	c := qt.New(t)
	today, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	var page bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdown(&page), qt.IsNil)
	bundled := withoutYDB{builtintest.Runtime()}
	documented, err := providerpublished.LintRuntime(bundled, bundled)
	c.Assert(err, qt.IsNil)

	entries, err := lintcatalog.EntriesFrom(documented)
	c.Assert(err, qt.IsNil)
	var rendered bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdownFrom(&rendered, documented), qt.IsNil)
	_, withoutRecord := lintcatalog.EntriesFrom(bundled)

	c.Assert(entries, qt.DeepEquals, today)
	c.Assert(rendered.String(), qt.Equals, page.String())
	c.Assert(withoutRecord, qt.ErrorMatches, `the catalog documents migration lint rule\(s\) YD.* that no longer exist: .*`)
}
