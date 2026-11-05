package providerpublished_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/providerpublished"
)

// ydbProvider is the official provider the YDB record was taken from.
const ydbProvider = "github.com/stokaro/ptah-ydb-plugin"

// servingYDB is a runtime that serves the ydb target itself, under both
// spellings, and publishes one rule of its own for it: the shape of a runtime
// a started YDB provider extends.
type servingYDB struct{ *engine.Runtime }

func newServingYDB(c *qt.C) servingYDB {
	c.Helper()
	runtime, err := engine.New(engine.Provider{
		ID:      ydbProvider,
		Targets: []engine.Target{{Name: "ydb", Aliases: []string{"ydbs"}}},
	})
	c.Assert(err, qt.IsNil)
	return servingYDB{runtime}
}

func (servingYDB) LintRules(target string) ([]lintext.Rule, error) {
	return []lintext.Rule{{Code: "YD101", Title: "served by the runtime", Severity: lintext.Error, Targets: []string{"ydb"}}}, nil
}

// published is what a rule list says on the documentation page.
type published struct {
	Code, Title   string
	Severity      lintext.Severity
	Targets       []string
	AppliesToDown bool
	Subsumes      []string
}

func publishedOf(rules []lintext.Rule) []published {
	out := make([]published, 0, len(rules))
	for _, rule := range rules {
		out = append(out, published{Code: rule.Code, Title: rule.Title, Severity: rule.Severity, Targets: rule.Targets,
			AppliesToDown: rule.AppliesToDown, Subsumes: rule.Subsumes})
	}
	return out
}

func TestDescriptions_RecordTheOfficialYDBProvider(t *testing.T) {
	c := qt.New(t)

	descriptions, err := providerpublished.Descriptions()

	c.Assert(err, qt.IsNil)
	c.Assert(descriptions, qt.HasLen, 1)
	c.Assert(descriptions[0].GetProviderId(), qt.Equals, ydbProvider)
	c.Assert(descriptions[0].GetEngines(), qt.HasLen, 1)
	c.Assert(descriptions[0].GetEngines()[0].GetId(), qt.Equals, "ydb")
	c.Assert(descriptions[0].GetEngines()[0].GetAliases(), qt.DeepEquals, []string{"ydbs"})
}

// The record speaks for YDB, which the bundle does not serve: every rule it
// lists targets ydb. Over a runtime that serves YDB it speaks for nothing.
func TestLintRules_RecordSpeaksForAnEngineTheBundleDoesNotServe(t *testing.T) {
	c := qt.New(t)

	recorded, err := providerpublished.LintRules(builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	served, err := providerpublished.LintRules(newServingYDB(c))
	c.Assert(err, qt.IsNil)

	c.Assert(recorded, qt.Not(qt.HasLen), 0)
	for _, rule := range recorded {
		c.Assert(rule.Targets, qt.DeepEquals, []string{"ydb"}, qt.Commentf("%s", rule.Code))
	}
	c.Assert(served, qt.HasLen, 0)
}

// The bundle, documented through the record, lists the recorded rules under
// either YDB spelling, and its own rules with the recorded ones in the whole
// catalog, ordered by code.
func TestLintRuntime_RestoresTheRulesOfAnEngineTheRuntimeDoesNotServe(t *testing.T) {
	bundled := builtintest.Runtime()
	recorded, err := providerpublished.LintRules(bundled)
	qt.New(t).Assert(err, qt.IsNil)
	own, err := bundled.LintRules("")
	qt.New(t).Assert(err, qt.IsNil)
	every := append(slices.Clone(own), recorded...)
	slices.SortFunc(every, func(a, b lintext.Rule) int { return strings.Compare(a.Code, b.Code) })
	for _, row := range []struct {
		name, target string
		want         []lintext.Rule
	}{
		{"every rule", "", every}, {"the engine ID", "ydb", recorded}, {"an alias", "ydbs", recorded},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := providerpublished.LintRuntime(bundled, bundled)
			c.Assert(err, qt.IsNil)

			got, err := runtime.LintRules(row.target)

			c.Assert(err, qt.IsNil)
			c.Assert(publishedOf(got), qt.DeepEquals, publishedOf(row.want))
		})
	}
}

// Over a runtime that serves YDB the record adds nothing, so the runtime's own
// registration stays the source.
func TestLintRuntime_AddsNothingWhereTheRuntimeServesTheEngine(t *testing.T) {
	c := qt.New(t)
	serving := newServingYDB(c)
	want, err := serving.LintRules("")
	c.Assert(err, qt.IsNil)
	runtime, err := providerpublished.LintRuntime(serving, serving)
	c.Assert(err, qt.IsNil)

	got, err := runtime.LintRules("")

	c.Assert(err, qt.IsNil)
	c.Assert(publishedOf(got), qt.DeepEquals, publishedOf(want))
}

// A rule the runtime publishes under a code the record publishes too is
// refused rather than listed twice: here the runtime publishes a YD rule while
// the resolver says nothing serves YDB.
func TestLintRuntime_FailurePath_ACodeBothPublish(t *testing.T) {
	c := qt.New(t)
	runtime, err := providerpublished.LintRuntime(newServingYDB(c), builtintest.Runtime())
	c.Assert(err, qt.IsNil)

	rules, err := runtime.LintRules("")

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `.*lint rule YD\d+ is published by the runtime and by a recorded provider`)
	c.Assert(rules, qt.IsNil)
}

// Over a runtime that serves YDB the record claims no directive. Over the
// bundle, which does not, the record claims directives of its own, each owned
// by the YDB provider.
func TestAnnotationSet_ClaimsTheDirectivesOfAnEngineTheBundleDoesNotServe(t *testing.T) {
	c := qt.New(t)

	served, err := providerpublished.AnnotationSet(newServingYDB(c))
	c.Assert(err, qt.IsNil)
	recorded, err := providerpublished.AnnotationSet(builtintest.Runtime())
	c.Assert(err, qt.IsNil)

	c.Assert(served.Selected(), qt.IsTrue)
	c.Assert(served.Directives(), qt.HasLen, 0)
	c.Assert(served.AttributedDirectives(), qt.HasLen, 0)
	c.Assert(recorded.Directives(), qt.Not(qt.HasLen), 0)
	for _, directive := range recorded.Directives() {
		owner, _ := recorded.Owner(directive.Name)
		c.Assert(owner, qt.Equals, ydbProvider, qt.Commentf("%s", directive.Name))
	}
	c.Assert(recorded.AttributedDirectives(), qt.Not(qt.HasLen), 0)
}

// A catalog of the bundle, which does not serve YDB, beside the record, knows
// the directives the record claims, as `ptah schema annotations` and the
// language server build theirs.
func TestAnnotationSet_JoinsTheCatalogOfARuntimeWithoutTheEngine(t *testing.T) {
	c := qt.New(t)
	recorded, err := providerpublished.AnnotationSet(builtintest.Runtime())
	c.Assert(err, qt.IsNil)

	catalog, err := annotationmeta.NewCatalog(annotation.None(), recorded)

	c.Assert(err, qt.IsNil)
	_, known := catalog.Lookup("ptah:schema:columnfamily")
	c.Assert(known, qt.IsTrue)
	c.Assert(catalog.AllowsAttribute("ptah:schema:table", "auto_partitioning_by_size"), qt.IsTrue)
	_, common := annotationmeta.Common().Lookup("ptah:schema:columnfamily")
	c.Assert(common, qt.IsFalse)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:table", "auto_partitioning_by_size"), qt.IsFalse)
}

// A declaration read through the recorded claims is refused, naming the
// provider to select: only a started provider reads its own directives.
func TestAnnotationSet_FailurePath_ReadingADeclaration(t *testing.T) {
	c := qt.New(t)
	recorded, err := providerpublished.AnnotationSet(builtintest.Runtime())
	c.Assert(err, qt.IsNil)

	contributions, err := recorded.Reader().Decode(annotation.Declaration{Directive: recorded.Directives()[0].Name})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*provider `+ydbProvider+` reads its Go annotations; select it to read this file`)
	c.Assert(contributions, qt.IsNil)
}
