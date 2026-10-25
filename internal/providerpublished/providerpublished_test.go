package providerpublished_test

import (
	"fmt"
	"slices"
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

// withoutYDB is the bundled runtime as it is once the bundle no longer
// carries the YDB engine: it resolves neither YDB spelling and publishes none
// of the YDB owner's rules. Every other answer is the bundle's.
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

// The record speaks for YDB only where the runtime does not serve it. While
// the bundle carries the YDB owner, the record and the owner publish the same
// rule catalog, so the page reads the same whichever is its source.
func TestLintRules_RecordAgreesWithTheBundledYDBOwner(t *testing.T) {
	c := qt.New(t)
	bundled := builtintest.Runtime()
	own, err := bundled.LintRules("ydb")
	c.Assert(err, qt.IsNil)

	served, err := providerpublished.LintRules(bundled)
	c.Assert(err, qt.IsNil)
	recorded, err := providerpublished.LintRules(withoutYDB{bundled})
	c.Assert(err, qt.IsNil)

	c.Assert(served, qt.HasLen, 0)
	c.Assert(recorded, qt.Not(qt.HasLen), 0)
	c.Assert(publishedOf(recorded), qt.DeepEquals, publishedOf(own))
}

// A runtime without the YDB owner, documented through the record, lists the
// rules the bundle lists today, under either YDB spelling as well as in the
// whole catalog.
func TestLintRuntime_RestoresTheRulesOfAnEngineTheRuntimeDoesNotServe(t *testing.T) {
	bundled := builtintest.Runtime()
	for _, row := range []struct{ name, target, bundledTarget string }{
		{"every rule", "", ""}, {"the engine ID", "ydb", "ydb"}, {"an alias", "ydbs", "ydb"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			want, err := bundled.LintRules(row.bundledTarget)
			c.Assert(err, qt.IsNil)
			runtime, err := providerpublished.LintRuntime(withoutYDB{bundled}, withoutYDB{bundled})
			c.Assert(err, qt.IsNil)

			got, err := runtime.LintRules(row.target)

			c.Assert(err, qt.IsNil)
			c.Assert(publishedOf(got), qt.DeepEquals, publishedOf(want))
		})
	}
}

// Over a runtime that serves YDB the record adds nothing, so the bundle's own
// registration stays the source.
func TestLintRuntime_AddsNothingWhereTheRuntimeServesTheEngine(t *testing.T) {
	c := qt.New(t)
	bundled := builtintest.Runtime()
	want, err := bundled.LintRules("")
	c.Assert(err, qt.IsNil)
	runtime, err := providerpublished.LintRuntime(bundled, bundled)
	c.Assert(err, qt.IsNil)

	got, err := runtime.LintRules("")

	c.Assert(err, qt.IsNil)
	c.Assert(publishedOf(got), qt.DeepEquals, publishedOf(want))
}

// A rule the runtime publishes under a code the record publishes too is
// refused rather than listed twice: here the runtime keeps the YDB owner's
// rules while saying it does not serve YDB.
func TestLintRuntime_FailurePath_ACodeBothPublish(t *testing.T) {
	c := qt.New(t)
	bundled := builtintest.Runtime()
	runtime, err := providerpublished.LintRuntime(bundled, withoutYDB{bundled})
	c.Assert(err, qt.IsNil)

	rules, err := runtime.LintRules("")

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `.*lint rule YD\d+ is published by the runtime and by a recorded provider`)
	c.Assert(rules, qt.IsNil)
}

// Over a runtime that serves YDB the record claims no directive. Over one
// that does not, every directive and attribute it claims is one the bundled
// catalog accepts: the record names nothing the bundle does not, so the
// annotation schema and the language server lose a YDB directive at the flip
// only where the provider does not claim it.
func TestAnnotationSet_ClaimsWhatTheBundledCatalogAccepts(t *testing.T) {
	c := qt.New(t)
	bundled := builtintest.Runtime()
	own, err := annotationmeta.NewCatalog(bundled.Annotations())
	c.Assert(err, qt.IsNil)

	served, err := providerpublished.AnnotationSet(bundled)
	c.Assert(err, qt.IsNil)
	recorded, err := providerpublished.AnnotationSet(withoutYDB{bundled})
	c.Assert(err, qt.IsNil)

	c.Assert(served.Selected(), qt.IsTrue)
	c.Assert(served.Directives(), qt.HasLen, 0)
	c.Assert(served.AttributedDirectives(), qt.HasLen, 0)
	c.Assert(recorded.Directives(), qt.Not(qt.HasLen), 0)
	for _, directive := range recorded.Directives() {
		owner, _ := recorded.Owner(directive.Name)
		c.Assert(owner, qt.Equals, ydbProvider)
		_, declared := own.Lookup(directive.Name)
		c.Assert(declared, qt.IsTrue, qt.Commentf("%s", directive.Name))
		for _, attribute := range directive.Attributes {
			c.Assert(own.AllowsAttribute(directive.Name, attribute.Name), qt.IsTrue, qt.Commentf("%s %s", directive.Name, attribute.Name))
		}
	}
	c.Assert(recorded.AttributedDirectives(), qt.Not(qt.HasLen), 0)
	for _, name := range recorded.AttributedDirectives() {
		for _, attribute := range recorded.Attributes(name) {
			c.Assert(own.AllowsAttribute(name, attribute.Name), qt.IsTrue, qt.Commentf("%s %s", name, attribute.Name))
		}
	}
}

// A catalog of a runtime without the YDB owner, beside the record, knows the
// directives the record claims, as `ptah schema annotations` and the language
// server build theirs.
func TestAnnotationSet_JoinsTheCatalogOfARuntimeWithoutTheEngine(t *testing.T) {
	c := qt.New(t)
	recorded, err := providerpublished.AnnotationSet(withoutYDB{builtintest.Runtime()})
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
	recorded, err := providerpublished.AnnotationSet(withoutYDB{builtintest.Runtime()})
	c.Assert(err, qt.IsNil)

	contributions, err := recorded.Reader().Decode(annotation.Declaration{Directive: recorded.Directives()[0].Name})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*provider `+ydbProvider+` reads its Go annotations; select it to read this file`)
	c.Assert(contributions, qt.IsNil)
}
