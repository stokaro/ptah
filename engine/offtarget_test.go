package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// skipFirst states that the first model is skipped where it is not served.
var skipFirst = engine.OffTargetPolicy{Kinds: []schemaext.Kind{conversionFirst}, Action: schemaext.OffTargetSkip, Label: "first model"}

// offTargetRuntime returns a runtime whose owner serves both models on
// custom alone and states policies, beside a provider of two more targets:
// elsewhere, which serves nothing, and strict, which refuses what an owner
// would skip there. rendered receives the schema elsewhere's render is given.
func offTargetRuntime(c *qt.C, rendered *renderer.SchemaRequest, policies ...engine.OffTargetPolicy) *engine.Runtime {
	c.Helper()
	provider := comparisonProvider(comparisonFunc(unchangedComparison))
	provider.OffTarget = policies
	render := schemaRenderFunc(func(_ context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		*rendered = request
		return renderer.SchemaResult{Complete: true, Statements: []string{"CREATE TABLE t ();"}}, nil
	})
	other := engine.Provider{ID: "example.org/other", Targets: []engine.Target{{Name: "elsewhere", SchemaRendering: render},
		{Name: "strict", RefusesSkips: true}}}
	return mustRuntime(c, provider, other)
}

// bothModels holds one object of each model.
func bothModels(c *qt.C) schemaext.Objects {
	c.Helper()
	return comparisonRequest(c).Desired.Objects
}

// TestScopeOffTarget_HappyPath pins what a target keeps: an object of a model
// it serves, and one whose owner states nothing, stay; one it does not serve
// whose owner skips it is left out and reported.
func TestScopeOffTarget_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := offTargetRuntime(c, new(renderer.SchemaRequest), skipFirst)
	objects := bothModels(c)

	elsewhere, skipped, err := runtime.ScopeOffTarget("elsewhere", objects)
	c.Assert(err, qt.IsNil)
	served, none, err := runtime.ScopeOffTarget("alternate", objects)
	c.Assert(err, qt.IsNil)

	c.Assert(elsewhere.Refs(), qt.DeepEquals, []objectidentity.ID{comparedRef(conversionSecond, "second")})
	c.Assert(skipped, qt.DeepEquals, []schemaext.SkippedObject{{Subject: comparedRef(conversionFirst, "first"), Label: "first model",
		Target: "elsewhere"}})
	c.Assert(skipped[0].Note(), qt.Equals, `first model public.orders.first is not supported by this target; skipped.`)
	c.Assert(served.Equal(objects), qt.IsTrue)
	c.Assert(none, qt.HasLen, 0)
	c.Assert(objects.Len(), qt.Equals, 2)
}

func TestScopeOffTarget_FailurePath(t *testing.T) {
	refuseFirst := engine.OffTargetPolicy{Kinds: []schemaext.Kind{conversionFirst}, Action: schemaext.OffTargetRefuse, Label: "first model"}
	tests := []struct {
		name   string
		policy engine.OffTargetPolicy
		target string
		is     error
		want   string
	}{
		{name: "an owner that refuses", policy: refuseFirst, target: "elsewhere", is: ptaherr.ErrUnsupportedFeature,
			want: `unsupported feature: first model public.orders.first cannot be planned on elsewhere; scope its declaration to the targets ` +
				`that host it, as dialects="custom" does in a Go annotation`},
		{name: "a target that refuses skips", policy: skipFirst, target: "strict", is: ptaherr.ErrUnsupportedFeature,
			want: `unsupported feature: first model public.orders.first cannot be planned on strict; .*`},
		{name: "an unknown target", policy: skipFirst, target: "unknown", is: ptaherr.ErrUnsupportedDialect, want: `.*"unknown"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := offTargetRuntime(c, new(renderer.SchemaRequest), test.policy)

			kept, skipped, err := runtime.ScopeOffTarget(test.target, bothModels(c))

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(kept.IsZero(), qt.IsTrue)
			c.Assert(skipped, qt.IsNil)
		})
	}
}

// TestOffTarget_RegistrationRefuses pins every statement a provider cannot
// make.
func TestOffTarget_RegistrationRefuses(t *testing.T) {
	tests := []struct {
		name     string
		policies []engine.OffTargetPolicy
		want     string
	}{
		{name: "an unknown answer", policies: []engine.OffTargetPolicy{{Kinds: skipFirst.Kinds, Action: "ignore", Label: "first model"}},
			want: `.*unknown off-target answer "ignore"`},
		{name: "no label", policies: []engine.OffTargetPolicy{{Kinds: skipFirst.Kinds, Action: schemaext.OffTargetSkip, Label: " "}},
			want: `.*without a label`},
		{name: "no model", policies: []engine.OffTargetPolicy{{Action: schemaext.OffTargetSkip, Label: "first model"}},
			want: `.*for no model`},
		{name: "a model it does not own", policies: []engine.OffTargetPolicy{{Kinds: []schemaext.Kind{"example.org/unowned"},
			Action: schemaext.OffTargetSkip, Label: "first model"}}, want: `.*model "example.org/unowned" it does not own`},
		{name: "two answers for one model", policies: []engine.OffTargetPolicy{skipFirst, skipFirst},
			want: `.*model "example.org/first" has two off-target answers`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := comparisonProvider(comparisonFunc(unchangedComparison))
			provider.OffTarget = test.policies

			runtime, err := engine.New(provider)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// TestCompareFeatures_SkipsAnObjectTheTargetDoesNotServe pins the
// comparison: a skipped object reaches no owner and comes back as skipped,
// once, from either side. Without the statement, the same comparison is
// refused for want of an owner.
func TestCompareFeatures_SkipsAnObjectTheTargetDoesNotServe(t *testing.T) {
	c := qt.New(t)
	first := must.Must(schemaext.NewObjects(schemaext.Object{Ref: comparedRef(conversionFirst, "first"),
		Value: &conversionValue{ID: conversionFirst, Number: 1}}))
	request := schemaext.ComparisonRequest{Target: "elsewhere", Owners: comparisonRequest(c).Parents,
		Desired: schemaext.FeatureState{Objects: first}, Current: schemaext.FeatureState{Objects: first}}

	result, err := offTargetRuntime(c, new(renderer.SchemaRequest), skipFirst).CompareFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	_, refused := offTargetRuntime(c, new(renderer.SchemaRequest)).CompareFeatures(t.Context(), request)

	c.Assert(result.Skipped, qt.DeepEquals, []schemaext.SkippedObject{{Subject: comparedRef(conversionFirst, "first"),
		Label: "first model", Target: "elsewhere"}})
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(refused, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(refused, qt.ErrorMatches, `.*no object comparison for "elsewhere"/"example.org/first"`)
}

// TestRenderSchema_WritesASkippedObject pins the render: the target's
// service never sees the object, and the result says it was skipped, first,
// as a statement and as an omission.
func TestRenderSchema_WritesASkippedObject(t *testing.T) {
	c := qt.New(t)
	received := new(renderer.SchemaRequest)
	runtime := offTargetRuntime(c, received, skipFirst)

	result, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "elsewhere",
		Schema: &schemamodel.Database{FeatureObjects: bothModels(c)}})

	c.Assert(err, qt.IsNil)
	c.Assert(received.Schema.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{comparedRef(conversionSecond, "second")})
	c.Assert(result.Statements, qt.DeepEquals, []string{`-- ELSEWHERE: first model public.orders.first is not supported by this target; skipped.`,
		"CREATE TABLE t ();"})
	c.Assert(result.Omissions, qt.DeepEquals, []renderer.Omission{{Dialect: "elsewhere", Reason: "unsupported", Kind: "first model",
		Name: "public.orders.first"}})
}

// TestRenderSchema_RefusesARefusedObject pins the render's refusal: a
// completed one, naming the targets that host the model, and no service call.
func TestRenderSchema_RefusesARefusedObject(t *testing.T) {
	c := qt.New(t)
	received := new(renderer.SchemaRequest)
	runtime := offTargetRuntime(c, received, engine.OffTargetPolicy{Kinds: []schemaext.Kind{conversionFirst},
		Action: schemaext.OffTargetRefuse, Label: "first model"})

	result, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "elsewhere",
		Schema: &schemamodel.Database{FeatureObjects: bothModels(c)}})

	c.Assert(err, qt.ErrorAs, new(*renderer.SchemaRefusalError))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*first model public.orders.first cannot be planned on elsewhere; .* as dialects="custom" does in a Go annotation`)
	c.Assert(result.Statements, qt.IsNil)
	c.Assert(received.Schema, qt.IsNil)
}

// TestCompareFeatures_DropsTheCoverageOfASkippedModel pins that a side's
// record about a skipped model reaches no owner: a subject the record leaves
// uninspected would otherwise ask the target for a comparison it does not
// have.
func TestCompareFeatures_DropsTheCoverageOfASkippedModel(t *testing.T) {
	c := qt.New(t)
	runtime := offTargetRuntime(c, new(renderer.SchemaRequest), skipFirst)
	uninspected := schemaext.SubjectCoverage{Kind: conversionFirst, Subject: comparedRef(conversionFirst, "first"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the read did not look"}}
	request := schemaext.ComparisonRequest{Target: "elsewhere", Owners: comparisonRequest(c).Parents,
		Desired: schemaext.FeatureState{Coverage: facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete, uninspected)}}

	result, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
}

// TestScopeOffTarget_ADeclaredModelIsServed pins that a target serves a model
// an owner only plans declarations of: a render-only model stays there, and
// is skipped where nothing serves it.
func TestScopeOffTarget_ADeclaredModelIsServed(t *testing.T) {
	c := qt.New(t)
	provider := declarationProvider(declarationFunc(func(context.Context, featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
		return featureplan.DeclarationResult{Complete: true}, nil
	}))
	provider.OffTarget = []engine.OffTargetPolicy{skipFirst}
	runtime := mustRuntime(c, provider, engine.Provider{ID: "example.org/other", Targets: []engine.Target{{Name: "elsewhere"}}})
	objects := bothModels(c)

	served, none, err := runtime.ScopeOffTarget("custom", objects)
	c.Assert(err, qt.IsNil)
	_, skipped, err := runtime.ScopeOffTarget("elsewhere", objects)
	c.Assert(err, qt.IsNil)

	c.Assert(served.Equal(objects), qt.IsTrue)
	c.Assert(none, qt.HasLen, 0)
	c.Assert(skipped, qt.HasLen, 1)
}

// TestCompareFeatures_NamesAnUndecidedModelByItsLabel pins that an undecided
// object carries the label its owner states for the model, the one its skip
// line uses, and none where the owner states none; Runtime.Label answers the
// same.
func TestCompareFeatures_NamesAnUndecidedModelByItsLabel(t *testing.T) {
	c := qt.New(t)
	provider := comparisonProvider(comparisonFunc(func(_ context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		return schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired, Undecided: []schemaext.UndecidedChange{
			{Kind: conversionFirst, Subject: comparedRef(conversionFirst, "first"), Reason: "unknown"},
			{Kind: conversionSecond, Subject: comparedRef(conversionSecond, "second"), Reason: "unknown"},
		}}, nil
	}))
	provider.OffTarget = []engine.OffTargetPolicy{skipFirst}
	runtime := mustRuntime(c, provider)
	objects := bothModels(c)

	result, err := runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{Target: "custom", Owners: comparisonRequest(c).Parents,
		Desired: schemaext.FeatureState{Objects: objects}, Current: schemaext.FeatureState{Objects: objects}})
	c.Assert(err, qt.IsNil)
	firstLabel, firstStated := runtime.Label(conversionFirst)
	secondLabel, secondStated := runtime.Label(conversionSecond)

	c.Assert(result.Undecided, qt.HasLen, 2)
	c.Assert(result.Undecided[0].Label, qt.Equals, "first model")
	c.Assert(result.Undecided[1].Label, qt.Equals, "")
	c.Assert(firstLabel, qt.Equals, skipFirst.Label)
	c.Assert(firstStated, qt.IsTrue)
	c.Assert(secondLabel, qt.Equals, "")
	c.Assert(secondStated, qt.IsFalse)
}
