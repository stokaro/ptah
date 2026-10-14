package pgtable_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func partitionRequest(desired *pgtable.DesiredPartition, current *pgtable.ObservedPartition) schemaext.FacetComparisonRequest {
	subject := persistenceSubject()
	return schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{pgtable.PartitionKind},
		Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(desired))}}, Coverage: must.Must(pgtable.PartitionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(current))}}, Coverage: must.Must(pgtable.PartitionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}}
}

func TestPartitionComparison_CapturedKeys(t *testing.T) {
	for _, test := range []struct {
		name               string
		declared, observed pgtable.PartitionPart
		normalized         string
		undecided          int
	}{
		{name: "same column", declared: pgtable.PartitionPart{Name: "tenant"}, observed: pgtable.PartitionPart{Name: "tenant"}},
		{name: "lexical expression", declared: pgtable.PartitionPart{Expr: "LOWER(word)"}, observed: pgtable.PartitionPart{Expr: `(lower("word"))`}},
		{name: "cast requires server", declared: pgtable.PartitionPart{Expr: "lower(word)"}, observed: pgtable.PartitionPart{Expr: "lower((word)::text)"}, undecided: 1},
		{name: "server resolves cast", declared: pgtable.PartitionPart{Expr: "lower(word)"}, observed: pgtable.PartitionPart{Expr: "lower((word)::text)"}, normalized: "lower((word)::text)"},
		{name: "collation requires server", declared: pgtable.PartitionPart{Name: "tenant", Collation: `"C"`}, observed: pgtable.PartitionPart{Name: "tenant"}, undecided: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{test.declared}}
			request := partitionRequest(desired, &pgtable.ObservedPartition{Type: "LIST", Parts: []pgtable.PartitionPart{test.observed}})
			normalized := map[string]*pgtable.NormalizedPartition{"lower((word)::text)": {SourceKey: pgtable.PartitionComparisonKey(&pgtable.Partition{Type: desired.Type, Parts: desired.Parts}), Key: pgtable.PartitionComparisonKey(&pgtable.Partition{Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: test.normalized}}})}}
			desired.Normalized = normalized[test.normalized]
			request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(desired))
			result, err := (pgtable.PartitionService{}).CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Desired, qt.DeepEquals, request.Desired)
		})
	}
}

func TestPartitionComparison_RefusesImmutableChanges(t *testing.T) {
	for _, desired := range []*pgtable.DesiredPartition{
		{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "other"}}},
		{Type: "HASH", Parts: []pgtable.PartitionPart{{Name: "tenant"}}},
		{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "tenant"}, {Name: "other"}}},
	} {
		c := qt.New(t)
		result, err := (pgtable.PartitionService{}).CompareFacets(t.Context(), partitionRequest(desired, &pgtable.ObservedPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "tenant"}}}))
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `.*: PostgreSQL table app\.cache partition key cannot change in place`)
		c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
	}
}

func TestPartitionComparison_UnknownIsNotAbsent(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		c := qt.New(t)
		request := partitionRequest(&pgtable.DesiredPartition{Type: "HASH", Parts: []pgtable.PartitionPart{{Name: "tenant"}}}, &pgtable.ObservedPartition{Type: "HASH", Parts: []pgtable.PartitionPart{{Name: "tenant"}}})
		request.Current.Records = nil
		request.Current.Coverage = must.Must(pgtable.PartitionCoverage(schemaext.Observed, schemaext.Knowledge{State: state, Reason: "not captured"}, nil))
		result, err := (pgtable.PartitionService{}).CompareFacets(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Undecided, qt.HasLen, 1)
		c.Assert(result.Changes, qt.HasLen, 0)
	}
}

func TestPartitionCodecs_RejectMalformedKeys(t *testing.T) {
	for _, codec := range pgtable.PartitionCodecs() {
		for _, raw := range []string{`null`, `{}`, `{"type":"HASH","parts":[{"name":"id","expr":" "}]}`, `{"type":"HASH","parts":[{"name":"id","collation":null}]}`, `{"type":"OTHER","parts":[{"name":"id"}]}`, `{"type":"RANGE","parts":[]}`, `{"type":"LIST","parts":[{"name":"a"},{"name":"b"}]}`, `{"type":"HASH","parts":[{"name":"id","expr":"id"}]}`, `{"type":"HASH","parts":[{"name":"id","extra":true}]}`, `{"type":"HASH","type":"LIST","parts":[{"name":"id"}]}`} {
			c := qt.New(t)
			value, err := codec.Decode(json.RawMessage(raw))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		}
	}
}

func TestPartitionConversion_DropsProbeFactsAndClonesParts(t *testing.T) {
	c := qt.New(t)
	desired := &pgtable.DesiredPartition{Type: "HASH", Parts: []pgtable.PartitionPart{{Name: "tenant"}}}
	key := pgtable.PartitionComparisonKey(&pgtable.Partition{Type: desired.Type, Parts: desired.Parts})
	desired.Normalized = &pgtable.NormalizedPartition{SourceKey: key, Key: key}
	service := pgtable.PartitionService{}
	observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{desired}})
	c.Assert(err, qt.IsNil)
	returned, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)
	c.Assert(returned[0].(*pgtable.DesiredPartition).Normalized, qt.IsNil)
	observed[0].(*pgtable.ObservedPartition).Parts[0].Name = "changed"
	c.Assert(desired.Parts[0].Name, qt.Equals, "tenant")
	c.Assert(returned[0].(*pgtable.DesiredPartition).Parts[0].Name, qt.Equals, "tenant")
}

func TestPartitionPlanning_RefusesUnknownPartitionTree(t *testing.T) {
	c := qt.New(t)
	request := persistencePlan()
	request.Changes = nil
	request.ParentKinds = []schemaext.Kind{pgtable.PartitionKind}
	request.Tables[0].Action = featureplan.RebuildTable
	request.Tables[0].Current.Table.Partitioned = true
	result, err := (pgtable.PartitionService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestPartitionServices_Cancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (pgtable.PartitionService{}).CompareFacets(ctx, schemaext.FacetComparisonRequest{Target: "postgres"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}
