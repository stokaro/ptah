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

// The official providers the records were taken from.
const (
	spannerProvider = "github.com/stokaro/ptah-spanner-plugin"
	ydbProvider     = "github.com/stokaro/ptah-ydb-plugin"
)

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

// newServingBoth is a runtime that serves both recorded engines itself, so no
// record speaks for anything over it.
func newServingBoth(c *qt.C) servingYDB {
	c.Helper()
	runtime, err := engine.New(
		engine.Provider{ID: ydbProvider, Targets: []engine.Target{{Name: "ydb", Aliases: []string{"ydbs"}}}},
		engine.Provider{ID: spannerProvider, Targets: []engine.Target{{Name: "spanner", Aliases: []string{"cloudspanner"}}}},
	)
	c.Assert(err, qt.IsNil)
	return servingYDB{runtime}
}

// targeting returns the rules that target name.
func targeting(rules []lintext.Rule, name string) []lintext.Rule {
	return slices.DeleteFunc(slices.Clone(rules), func(rule lintext.Rule) bool { return !slices.Equal(rule.Targets, []string{name}) })
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

// The records are the official Spanner and YDB providers, ordered by provider
// ID, each with its one engine and the aliases the catalog reserves.
func TestDescriptions_RecordTheOfficialProviders(t *testing.T) {
	c := qt.New(t)

	descriptions, err := providerpublished.Descriptions()

	c.Assert(err, qt.IsNil)
	c.Assert(descriptions, qt.HasLen, 2)
	c.Assert(descriptions[0].GetProviderId(), qt.Equals, spannerProvider)
	c.Assert(descriptions[0].GetEngines(), qt.HasLen, 1)
	c.Assert(descriptions[0].GetEngines()[0].GetId(), qt.Equals, "spanner")
	c.Assert(descriptions[0].GetEngines()[0].GetAliases(), qt.DeepEquals, []string{"cloudspanner", "google-spanner", "google_spanner"})
	c.Assert(descriptions[1].GetProviderId(), qt.Equals, ydbProvider)
	c.Assert(descriptions[1].GetEngines(), qt.HasLen, 1)
	c.Assert(descriptions[1].GetEngines()[0].GetId(), qt.Equals, "ydb")
	c.Assert(descriptions[1].GetEngines()[0].GetAliases(), qt.DeepEquals, []string{"ydbs"})
}

// The records speak for Spanner and YDB, which the bundle does not serve: every
// rule targets one of the two, the SP family Spanner and the YD family YDB.
// Over a runtime that serves YDB only the Spanner record speaks, and over one
// that serves both, nothing does.
func TestLintRules_RecordSpeaksForAnEngineTheBundleDoesNotServe(t *testing.T) {
	c := qt.New(t)

	recorded, err := providerpublished.LintRules(builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	servedYDB, err := providerpublished.LintRules(newServingYDB(c))
	c.Assert(err, qt.IsNil)
	servedBoth, err := providerpublished.LintRules(newServingBoth(c))
	c.Assert(err, qt.IsNil)

	families := make(map[string][]string)
	for _, rule := range recorded {
		families[rule.Code[:2]] = rule.Targets
	}
	c.Assert(families, qt.DeepEquals, map[string][]string{"SP": {"spanner"}, "YD": {"ydb"}})
	c.Assert(publishedOf(servedYDB), qt.DeepEquals, publishedOf(targeting(recorded, "spanner")))
	c.Assert(targeting(recorded, "spanner"), qt.Not(qt.HasLen), 0)
	c.Assert(servedBoth, qt.HasLen, 0)
}

// The bundle, documented through the records, lists each engine's recorded
// rules under its ID and an alias, and its own rules with the recorded ones in
// the whole catalog, ordered by code.
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
		{"every rule", "", every},
		{"the YDB engine ID", "ydb", targeting(recorded, "ydb")}, {"a YDB alias", "ydbs", targeting(recorded, "ydb")},
		{"the Spanner engine ID", "spanner", targeting(recorded, "spanner")},
		{"a Spanner alias", "cloudspanner", targeting(recorded, "spanner")},
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

// Over a runtime that serves both engines the records add nothing, so the
// runtime's own registration stays the source.
func TestLintRuntime_AddsNothingWhereTheRuntimeServesTheEngine(t *testing.T) {
	c := qt.New(t)
	serving := newServingBoth(c)
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

// Over a runtime that serves both engines the records claim no directive. Over
// the bundle, which serves neither, the YDB record claims directives of its
// own, each owned by the YDB provider; the Spanner record claims only
// attributes of a common directive.
func TestAnnotationSet_ClaimsTheDirectivesOfAnEngineTheBundleDoesNotServe(t *testing.T) {
	c := qt.New(t)

	served, err := providerpublished.AnnotationSet(newServingBoth(c))
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
