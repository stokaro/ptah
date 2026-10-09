package chcompare_test

import (
	"context"
	"math"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func indexRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse-index", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: append(chschema.IndexCodecs(), chdiff.IndexCodecs()...),
		FacetComparisons: []engine.FacetComparison{{Target: "clickhouse", OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
			Kinds: []schemaext.Kind{chschema.IndexKind}, ChangeKinds: []schemaext.Kind{chdiff.IndexKind}, Service: chcompare.IndexService{}}},
	}))
}

func indexRequest(before, after *chschema.ObservedIndex) schemaext.FacetComparisonRequest {
	ref := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).IndexParts("db.name", "events.name", "by.status")
	return schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.IndexKind},
		Owners:  []schemaext.ParentState{{Subject: ref, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: ref, Values: must.Must(schemaext.NewFacets(after.Desired()))}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: ref, Values: must.Must(schemaext.NewFacets(before))}}},
	}
}

func TestIndexComparisonCapturesSettingsAndPreservesSource(t *testing.T) {
	before := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}
	for _, after := range []*chschema.ObservedIndex{
		{IndexType: "set(200)", Granularity: 4},
		{IndexType: "set(100)", Granularity: math.MaxUint64},
		{IndexType: "bloom_filter(0.01)", Granularity: 64},
	} {
		c := qt.New(t)
		request := indexRequest(before, after)
		result, err := indexRuntime().CompareFacets(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Complete, qt.IsTrue)
		c.Assert(result.Undecided, qt.HasLen, 0)
		c.Assert(result.Changes, qt.HasLen, 1)
		c.Assert(result.Changes[0].Kind, qt.Equals, chschema.IndexKind)
		c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, request.Owners[0].Subject)
		change := result.Changes[0].Change.Value.(*chdiff.Index)
		c.Assert(change, qt.DeepEquals, &chdiff.Index{Before: before, After: after.Desired()})
		change.Before.IndexType = "mutated"
		c.Assert(before.IndexType, qt.Equals, "set(100)")
		c.Assert(result.Desired, qt.DeepEquals, request.Desired)
	}
}

func TestIndexComparisonUsesTokenSemantics(t *testing.T) {
	for _, test := range []struct {
		name, before, after string
		changes             int
	}{
		{"spacing", "bloom_filter(0.01)", "bloom_filter ( 0.01 )", 0},
		{"parameters", "tokenbf_v1(256,2,0)", "tokenbf_v1(256,3,0)", 1},
		{"parameter order", "tokenbf_v1(256,2,0)", "tokenbf_v1(256,0,2)", 1},
		{"quoted text", "custom('New York')", "custom('NewYork')", 1},
		{"token boundaries", "custom(foo bar)", "custom(foobar)", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest(&chschema.ObservedIndex{IndexType: test.before, Granularity: 1}, &chschema.ObservedIndex{IndexType: test.after, Granularity: 1})
			result, err := indexRuntime().CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

func TestIndexComparisonAdoptsAnUnmanagedObservation(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}
	request := indexRequest(before, before)
	request.Desired.Records[0].Values = schemaext.Facets{}
	result, err := indexRuntime().CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, found, err := schemaext.FacetAs[*chschema.DesiredIndex](result.Desired.Records[0].Values, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted, qt.DeepEquals, before.Desired())
	c.Assert(request.Desired.Records[0].Values.IsZero(), qt.IsTrue)
}

func indexCoverage(runtime *engine.Runtime, representation schemaext.Representation, subject objectidentity.ID, state schemaext.KnowledgeState) schemaext.Coverage {
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.IndexKind && v.Representation == representation
	})]
	return must.Must(schemaext.NewCoverage(representation,
		[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
		[]schemaext.SubjectCoverage{{Kind: chschema.IndexKind, Subject: subject, Knowledge: schemaext.Knowledge{State: state, Reason: "source knowledge"}}},
	))
}

func TestIndexComparisonPreservesKnowledgeLimits(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		limit          func(*schemaext.FacetComparisonRequest, schemaext.Coverage)
	}{
		{"desired", schemaext.Desired, func(r *schemaext.FacetComparisonRequest, coverage schemaext.Coverage) { r.Desired.Coverage = coverage }},
		{"current", schemaext.Observed, func(r *schemaext.FacetComparisonRequest, coverage schemaext.Coverage) { r.Current.Coverage = coverage }},
	} {
		for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
			t.Run(test.name+"/"+string(state), func(t *testing.T) {
				c := qt.New(t)
				runtime := indexRuntime()
				request := indexRequest(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1})
				test.limit(&request, indexCoverage(runtime, test.representation, request.Owners[0].Subject, state))
				result, err := runtime.CompareFacets(t.Context(), request)
				c.Assert(err, qt.IsNil)
				c.Assert(result.Complete, qt.IsTrue)
				c.Assert(result.Changes, qt.HasLen, 0)
				c.Assert(result.Undecided, qt.HasLen, 1)
				c.Assert(result.Desired, qt.DeepEquals, request.Desired)
			})
		}
	}
}

func TestIndexComparisonLeavesLifecycleAndUnmentionedSettingsAlone(t *testing.T) {
	for _, edit := range []func(*schemaext.FacetComparisonRequest){
		func(r *schemaext.FacetComparisonRequest) {
			r.Owners[0].Current = false
			r.Current = schemaext.FacetState{}
		},
		func(r *schemaext.FacetComparisonRequest) {
			r.Owners[0].Desired = false
			r.Desired = schemaext.FacetState{}
		},
		func(r *schemaext.FacetComparisonRequest) {
			r.Desired, r.Current = schemaext.FacetState{}, schemaext.FacetState{}
		},
	} {
		c := qt.New(t)
		value := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
		request := indexRequest(value, value)
		edit(&request)
		result, err := indexRuntime().CompareFacets(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 0)
		c.Assert(result.Undecided, qt.HasLen, 0)
	}
}

func TestIndexComparisonDoesNotInferMissingObservation(t *testing.T) {
	c := qt.New(t)
	value := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	request := indexRequest(value, value)
	request.Current = schemaext.FacetState{}
	result, err := indexRuntime().CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
}

func TestIndexComparisonHonorsExclusions(t *testing.T) {
	c := qt.New(t)
	request := indexRequest(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1})
	request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(chschema.IndexKind, "postgres"))
	result, err := indexRuntime().CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	c.Assert(result.Desired.Records[0].Values.Kinds(), qt.HasLen, 0)
	c.Assert(result.Desired.Records[0].Values.DeclaredKinds(), qt.DeepEquals, []schemaext.Kind{chschema.IndexKind})
}

func TestIndexComparisonRefusesMissingResolvedIntent(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Absent, schemaext.Complete, schemaext.Defaulted} {
		c := qt.New(t)
		runtime := indexRuntime()
		value := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
		request := indexRequest(value, value)
		request.Desired.Records[0].Values = schemaext.Facets{}
		request.Desired.Coverage = indexCoverage(runtime, schemaext.Desired, request.Owners[0].Subject, state)
		result, err := runtime.CompareFacets(t.Context(), request)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
	}
}

func TestIndexComparisonRefusesContradictoryAbsence(t *testing.T) {
	c := qt.New(t)
	runtime := indexRuntime()
	value := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	request := indexRequest(value, value)
	request.Current.Coverage = indexCoverage(runtime, schemaext.Observed, request.Owners[0].Subject, schemaext.Absent)
	result, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}

func TestIndexComparisonRefusesInvalidRequestsAtomically(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*schemaext.FacetComparisonRequest)
		want error
	}{
		{"target", func(r *schemaext.FacetComparisonRequest) { r.Target = "postgres" }, ptaherr.ErrUnsupportedDialect},
		{"kinds", func(r *schemaext.FacetComparisonRequest) { r.Kinds = []schemaext.Kind{chschema.TableKind} }, schemaext.ErrInvalidValue},
		{"parent kind", func(r *schemaext.FacetComparisonRequest) { r.Owners[0].Subject.Kind = objectidentity.KindTable }, schemaext.ErrInvalidValue},
		{"duplicate owner", func(r *schemaext.FacetComparisonRequest) { r.Owners = append(r.Owners, r.Owners[0]) }, schemaext.ErrInvalidValue},
		{"unresolved desired", func(r *schemaext.FacetComparisonRequest) {
			r.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&chschema.DesiredIndex{}))
		}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1})
			test.edit(&request)
			result, err := (chcompare.IndexService{}).CompareFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
		})
	}
}

func TestIndexComparisonHonorsCancellationAndRequiresContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (chcompare.IndexService{}).CompareFacets(ctx, schemaext.FacetComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
	var missingContext context.Context
	result, err = (chcompare.IndexService{}).CompareFacets(missingContext, schemaext.FacetComparisonRequest{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}
