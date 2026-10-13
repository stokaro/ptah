package pgroutine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgroutine"
)

func propertySubject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).Function("f", "integer")
}
func propertyCoverage(representation schemaext.Representation, state schemaext.KnowledgeState) schemaext.Coverage {
	return must.Must(pgroutine.PropertiesCoverage(representation, schemaext.Knowledge{State: state, Reason: "source knowledge"}, nil))
}
func TestPropertiesCodecs_HappyPath(t *testing.T) {
	for _, codec := range pgroutine.PropertiesCodecs() {
		t.Run(string(codec.Representation), func(t *testing.T) {
			c := qt.New(t)
			value, err := codec.Decode(json.RawMessage(`{"leakproof":true,"parallel":"SAFE","strict":true}`))
			c.Assert(err, qt.IsNil)
			encoded, err := codec.Encode(value)
			c.Assert(err, qt.IsNil)
			again, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, value)
			copied, err := codec.Clone(value)
			c.Assert(err, qt.IsNil)
			c.Assert(copied, qt.DeepEquals, value)
			c.Assert(copied, qt.Not(qt.Equals), value)
		})
	}
}
func TestPropertiesCodecs_FailurePath(t *testing.T) {
	for _, codec := range pgroutine.PropertiesCodecs() {
		for _, text := range []string{`null`, `{}`, `{"strict":false}`, `{"strict":null}`, `{"parallel":"UNSAFE"}`, `{"parallel":"safe"}`, `{"parallel":"FAST"}`, `{"strict":true,"extra":1}`, `{"strict":true,"strict":true}`} {
			t.Run(string(codec.Representation)+text, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(text))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}
func TestPropertiesChangeCodec_HappyPath(t *testing.T) {
	for _, change := range []*pgroutine.PropertiesChange{
		{After: &pgroutine.DesiredProperties{Strict: true}},
		{Before: &pgroutine.ObservedProperties{Parallel: "SAFE"}},
		{Before: &pgroutine.ObservedProperties{Strict: true}, After: &pgroutine.DesiredProperties{Leakproof: true}},
	} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			c := qt.New(t)
			codec := pgroutine.PropertiesChangeCodec()
			data, err := codec.Encode(change)
			c.Assert(err, qt.IsNil)
			got, err := codec.Decode(data)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, change)
			inverse, err := change.Reverse()
			c.Assert(err, qt.IsNil)
			restored, err := inverse.Reverse()
			c.Assert(err, qt.IsNil)
			c.Assert(restored, qt.DeepEquals, change)
			copied := change.Copy()
			c.Assert(copied, qt.Not(qt.Equals), change)
			c.Assert(copied, qt.DeepEquals, change)
		})
	}
}
func TestPropertiesChangeCodec_FailurePath(t *testing.T) {
	for _, text := range []string{`null`, `{}`, `{"before":null,"after":null}`, `{"after":{"strict":true}}`, `{"before":null,"after":{"strict":false}}`, `{"before":{"strict":true},"after":{"strict":true}}`, `{"before":null,"after":{"strict":true},"extra":1}`} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgroutine.PropertiesChangeCodec().Decode(json.RawMessage(text))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}
func TestPropertiesService_ConversionAndReversal(t *testing.T) {
	for _, target := range []string{"postgres", "cockroachdb", "yugabytedb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			service := pgroutine.PropertiesService{}
			declared := &pgroutine.DesiredProperties{Strict: true, Parallel: "SAFE"}
			observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: target, From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declared}})
			c.Assert(err, qt.IsNil)
			restored, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: target, From: schemaext.Observed, To: schemaext.Desired, Values: observed})
			c.Assert(err, qt.IsNil)
			c.Assert(restored, qt.DeepEquals, []schemaext.Value{declared})
			restored[0].(*pgroutine.DesiredProperties).Strict = false
			c.Assert(declared.Strict, qt.IsTrue)
			change := &pgroutine.PropertiesChange{Before: &pgroutine.ObservedProperties{Leakproof: true}, After: declared}
			reverse, err := service.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: target, Changes: []schemaext.ChangeRecord{{Subject: propertySubject(), Value: change}}})
			c.Assert(err, qt.IsNil)
			c.Assert(reverse, qt.HasLen, 1)
			c.Assert(reverse[0].Change.Value, qt.DeepEquals, &pgroutine.PropertiesChange{Before: &pgroutine.ObservedProperties{Strict: true, Parallel: "SAFE"}, After: &pgroutine.DesiredProperties{Leakproof: true}})
			c.Assert(reverse[0].ForwardState[0].Value, qt.DeepEquals, &pgroutine.ObservedProperties{Strict: true, Parallel: "SAFE"})
		})
	}
}
func TestPropertiesService_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		target string
		want   error
	}{
		{"nil context", nil, "postgres", schemaext.ErrInvalidValue},
		{"canceled", canceled, "postgres", context.Canceled},
		{"other engine", t.Context(), "mysql", ptaherr.ErrUnsupportedDialect},
		{"spanner", t.Context(), "spanner", ptaherr.ErrUnsupportedDialect},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := pgroutine.PropertiesService{}
			converted, err := service.ConvertFeatures(test.ctx, schemaext.ConversionRequest{Target: test.target, From: schemaext.Desired, To: schemaext.Observed})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(converted, qt.IsNil)
			compared, err := service.CompareFacets(test.ctx, schemaext.FacetComparisonRequest{Target: test.target})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(compared, qt.DeepEquals, schemaext.FacetComparisonResult{})
			reversed, err := service.ReverseChanges(test.ctx, schemaext.ReversalRequest{Target: test.target})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(reversed, qt.IsNil)
		})
	}
}
func TestPropertiesService_PartialBatchIsDiscarded(t *testing.T) {
	c := qt.New(t)
	values, err := (pgroutine.PropertiesService{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&pgroutine.DesiredProperties{Strict: true}, &pgroutine.ObservedProperties{Strict: true}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(values, qt.IsNil)
}
func TestPropertiesComparison_Knowledge(t *testing.T) {
	declared := must.Must(pgroutine.WithProperties(schemaext.Facets{}, pgroutine.Properties{Strict: true}))
	observed := must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, pgroutine.Properties{Leakproof: true}))
	for _, test := range []struct {
		name               string
		desired, current   schemaext.Coverage
		declared, observed schemaext.Facets
		created, removed   bool
		changes, undecided int
	}{
		{name: "known removal", desired: propertyCoverage(schemaext.Desired, schemaext.Complete), current: propertyCoverage(schemaext.Observed, schemaext.Complete), observed: observed, changes: 1},
		{name: "unmanaged keeps current", observed: observed},
		{name: "unknown current", desired: propertyCoverage(schemaext.Desired, schemaext.Complete), declared: declared, undecided: 1},
		{name: "unknown declaration", desired: propertyCoverage(schemaext.Desired, schemaext.Unrepresentable), declared: declared, observed: observed, undecided: 1},
		{name: "unknown observed value", current: propertyCoverage(schemaext.Observed, schemaext.Uninspected), declared: declared, observed: observed, undecided: 1},
		{name: "created host owns settings", declared: declared, created: true},
		{name: "removed host owns settings", observed: observed, removed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{pgroutine.PropertiesKind}, Desired: schemaext.FacetState{Coverage: test.desired}, Current: schemaext.FacetState{Coverage: test.current}, Owners: []schemaext.ParentState{{Subject: propertySubject(), Desired: !test.removed, Current: !test.created}}}
			request.Desired.Records = []schemaext.FacetRecord{{Subject: propertySubject(), Values: test.declared}}
			request.Current.Records = []schemaext.FacetRecord{{Subject: propertySubject(), Values: test.observed}}

			got, err := (pgroutine.PropertiesService{}).CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
			c.Assert(got.Changes, qt.HasLen, test.changes)
			c.Assert(got.Undecided, qt.HasLen, test.undecided)
		})
	}
}
func ExampleProperties() {
	properties := must.Must((pgroutine.Properties{Strict: true, Parallel: "safe"}).Normalize())
	fmt.Println(properties.Clauses())
	// Output: [STRICT PARALLEL SAFE]
}
