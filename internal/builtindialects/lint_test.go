package builtindialects_test

import (
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestLint_AgreesWithTheSwitchesItReplaced pins what each built-in descriptor
// tells migration lint, as the dialect switches in internal/lintdialect,
// internal/lintscan and migration/lint decided it. The message texts are
// pinned by the rules' own tests; this pins which dialect gets which
// behavior.
func TestLint_AgreesWithTheSwitchesItReplaced(t *testing.T) {
	want := map[string]struct {
		analyzed, notNull, byType, rebuilds, online bool
		scanner, dynamicSQL, types, atlas           string
		listed                                      []string
	}{
		"postgres": {analyzed: true, scanner: "dollar", dynamicSQL: "execute", online: true, types: "pgtype", atlas: "PG",
			listed: []string{"type", "view", "materialized_view", "trigger", "sequence"}},
		"mysql": {analyzed: true, scanner: "hash", dynamicSQL: "prepare-from", notNull: true, online: true, types: "mytype", atlas: "MY",
			listed: []string{"view", "trigger"}},
		"mariadb":     {analyzed: true, scanner: "hash", dynamicSQL: "prepare-from", notNull: true, online: true, types: "mytype", atlas: "MY"},
		"clickhouse":  {analyzed: true, notNull: true, byType: true},
		"sqlite":      {analyzed: true, rebuilds: true, listed: []string{"view", "trigger"}},
		"sqlserver":   {analyzed: true, dynamicSQL: "exec", notNull: true, types: "restated"},
		"cockroachdb": {analyzed: true},
		"yugabytedb":  {analyzed: true},
		"oracle":      {},
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			row, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			lint := dialect.Lint
			c.Assert(lint.Analyzed, qt.Equals, row.analyzed)
			c.Assert(lint.Scanner, qt.Equals, row.scanner)
			c.Assert(lint.DynamicSQL, qt.Equals, row.dynamicSQL)
			c.Assert(lint.DynamicSQL != "", qt.Equals, lint.InjectionAdvice != "")
			c.Assert(lint.NotNullConsequence != "", qt.Equals, row.notNull)
			c.Assert(lint.NotNullByType, qt.Equals, row.byType)
			c.Assert(lint.SetNotNullRebuilds, qt.Equals, row.rebuilds)
			c.Assert(lint.OnlineMeasured, qt.Equals, row.online)
			c.Assert(lint.TypeRestatements, qt.Equals, row.types)
			c.Assert(lint.AtlasAnalyzer, qt.Equals, row.atlas)
			c.Assert(lint.ListedObjects, qt.DeepEquals, row.listed)
		})
	}
}

// Every kind of object a descriptor says its read lists is one a lint
// baseline names, once: lint looks a kind up by that name, so a misspelled one
// would list nothing and read as a decision not to.
func TestLint_ListedObjectsAreBaselineKinds(t *testing.T) {
	var kinds []string
	for _, kind := range lintext.BaselineObjectKinds() {
		kinds = append(kinds, string(kind))
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			listed := dialect.Lint.ListedObjects
			for _, kind := range listed {
				c.Assert(kinds, qt.Contains, kind)
			}
			c.Assert(slices.Compact(slices.Sorted(slices.Values(listed))), qt.HasLen, len(listed))
		})
	}
}

// A dialect lint does not analyze states nothing else about linting it:
// nothing would read it.
func TestLint_AnUnanalyzedDialectStatesNothing(t *testing.T) {
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dialect.Lint.Analyzed || reflect.DeepEqual(dialect.Lint, descriptor.Lint{}), qt.IsTrue)
		})
	}
}
