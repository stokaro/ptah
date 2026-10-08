package chcompare_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func comparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:               "example.org/clickhouse",
		Targets:          []engine.Target{{Name: "clickhouse"}},
		Codecs:           append(chschema.Codecs(), chdiff.Codecs()...),
		FacetComparisons: []engine.FacetComparison{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind}, ChangeKinds: []schemaext.Kind{chdiff.TableKind}, Service: chcompare.Service{}}},
	}))
}

func tableRequest(before, after *chschema.ObservedTable) schemaext.FacetComparisonRequest {
	ref := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts("", "events")
	return schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind},
		Owners:  []schemaext.ParentState{{Subject: ref, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: ref, Values: must.Must(schemaext.NewFacets(after.Desired()))}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: ref, Values: must.Must(schemaext.NewFacets(before))}}},
	}
}

func TestTableComparisonCapturesEveryChangedSetting(t *testing.T) {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, ts", PrimaryKey: "id", TTL: "ts + toIntervalDay(7)"}
	for _, test := range []struct {
		name   string
		change func(*chschema.ObservedTable)
	}{
		{name: "engine", change: func(v *chschema.ObservedTable) { v.Engine = "ReplacingMergeTree" }},
		{name: "sorting key", change: func(v *chschema.ObservedTable) { v.OrderBy = "ts, id" }},
		{name: "empty sparse key", change: func(v *chschema.ObservedTable) { v.PrimaryKey = "" }},
		{name: "partition", change: func(v *chschema.ObservedTable) { v.PartitionBy = "toYYYYMM(ts)" }},
		{name: "sampling", change: func(v *chschema.ObservedTable) { v.SampleBy = "id" }},
		{name: "remove TTL", change: func(v *chschema.ObservedTable) { v.TTL = "" }},
		{name: "settings", change: func(v *chschema.ObservedTable) { v.Settings = "index_granularity = 4096" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			after := new(*before)
			test.change(after)
			request := tableRequest(before, after)
			runtime := comparisonRuntime()
			result, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, request.Owners[0].Subject)
			change := result.Changes[0].Change.Value.(*chdiff.Table)
			c.Assert(change.Before, qt.DeepEquals, before)
			c.Assert(change.After, qt.DeepEquals, after.Desired())
			change.Before.Engine = "mutated"
			c.Assert(before.Engine, qt.Equals, "MergeTree")
			c.Assert(result.Desired, qt.DeepEquals, request.Desired)
		})
	}
}

func TestTableComparisonRecognizesEquivalentForms(t *testing.T) {
	for _, test := range []struct {
		name          string
		before, after chschema.ObservedTable
	}{
		{name: "outer parentheses", before: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "(id, ts)", PrimaryKey: "(id)"}, after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id,ts", PrimaryKey: "id"}},
		{name: "empty tuple", before: chschema.ObservedTable{Engine: "MergeTree"}, after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", PrimaryKey: "tuple()"}},
		{name: "expression whitespace", before: chschema.ObservedTable{Engine: "ReplacingMergeTree(version)", TTL: "ts + toIntervalDay(7)"}, after: chschema.ObservedTable{Engine: "ReplacingMergeTree ( version )", TTL: "ts+toIntervalDay( 7 )"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := comparisonRuntime().CompareFacets(t.Context(), tableRequest(&test.before, &test.after))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

func TestTableComparisonPreservesExpressionMeaning(t *testing.T) {
	for _, test := range []struct{ name, before, after string }{
		{name: "literal whitespace", before: "city = 'New York'", after: "city = 'NewYork'"},
		{name: "identifier case", before: "UserID", after: "userid"},
		{name: "inner parentheses", before: "(a + b) * c", after: "a + b * c"},
		{name: "key order", before: "a,b", after: "b,a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: test.before}
			after := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: test.after}
			result, err := comparisonRuntime().CompareFacets(t.Context(), tableRequest(before, after))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
		})
	}
}

func TestTableComparisonAdoptsUnmanagedObservation(t *testing.T) {
	c := qt.New(t)
	current := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: ""}
	request := tableRequest(current, current)
	request.Desired = schemaext.FacetState{}
	result, err := comparisonRuntime().CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, found, err := schemaext.FacetAs[*chschema.DesiredTable](result.Desired.Records[0].Values, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted, qt.DeepEquals, current.Desired())
}

func TestTableComparisonDoesNotManageAnUnmentionedTable(t *testing.T) {
	c := qt.New(t)
	runtime := comparisonRuntime()
	value := &chschema.ObservedTable{Engine: "Memory"}
	request := tableRequest(value, value)
	request.Desired, request.Current = schemaext.FacetState{}, schemaext.FacetState{}
	result, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 0)
}

func TestTableComparisonPreservesExplicitLimitsWithoutValues(t *testing.T) {
	c := qt.New(t)
	runtime := comparisonRuntime()
	value := &chschema.ObservedTable{Engine: "Memory"}
	request := tableRequest(value, value)
	request.Desired, request.Current = schemaext.FacetState{}, schemaext.FacetState{}
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	request.Current.Coverage = must.Must(schemaext.NewCoverage(schemaext.Observed,
		[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
		[]schemaext.SubjectCoverage{{Kind: chschema.TableKind, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "inspection failed"}}},
	))
	result, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Desired.Records, qt.HasLen, 0)
}

func TestTableComparisonRetainsUnknownState(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			runtime := comparisonRuntime()
			current := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
			request := tableRequest(current, &chschema.ObservedTable{Engine: "Memory"})
			models := runtime.Codecs().Definitions()
			model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
				return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
			})]
			request.Current.Coverage = must.Must(schemaext.NewCoverage(schemaext.Observed,
				[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
				[]schemaext.SubjectCoverage{{Kind: chschema.TableKind, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: state, Reason: "incomplete inspection"}}},
			))
			result, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Subject, qt.DeepEquals, request.Owners[0].Subject)
		})
	}
}

func TestTableComparisonDoesNotOwnCreationOrRemoval(t *testing.T) {
	c := qt.New(t)
	current := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
	create := tableRequest(current, current)
	create.Owners[0].Current = false
	create.Current = schemaext.FacetState{}
	remove := tableRequest(current, current)
	remove.Owners[0].Desired = false
	remove.Desired = schemaext.FacetState{}
	for _, request := range []schemaext.FacetComparisonRequest{create, remove} {
		result, err := comparisonRuntime().CompareFacets(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 0)
		c.Assert(result.Undecided, qt.HasLen, 0)
	}
}

func TestTableComparisonRefusesUnresolvedIntent(t *testing.T) {
	c := qt.New(t)
	current := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
	request := tableRequest(current, current)
	request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&chschema.DesiredTable{}))
	result, err := comparisonRuntime().CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}

func TestTableComparisonHonorsCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (chcompare.Service{}).CompareFacets(ctx, schemaext.FacetComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}
