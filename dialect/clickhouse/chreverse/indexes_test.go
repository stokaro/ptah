package chreverse_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func indexChange() schemaext.ChangeRecord {
	return schemaext.ChangeRecord{
		Subject: objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).IndexParts("db.name", "events.name", "by.status"),
		Value: &chdiff.Index{
			Before: &chschema.ObservedIndex{IndexType: "set ( 100 )", Granularity: math.MaxUint64},
			After:  (&chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 4}).Desired(),
		},
	}
}

func TestIndexReversalReconstructsDefinitionAndReportsMaterializationLimit(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/index-reversal", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs:    append(chschema.IndexCodecs(), chdiff.IndexCodecs()...),
		Reversals: []engine.Reversal{{Target: "clickhouse", Kinds: []schemaext.Kind{chdiff.IndexKind}, Service: chreverse.IndexService{}}},
	}))
	record := indexChange()
	request := schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{record}}
	result, err := runtime.ReverseChanges(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Change.Subject, qt.DeepEquals, record.Subject)
	inverse := result[0].Change.Value.(*chdiff.Index)
	c.Assert(inverse.Before, qt.DeepEquals, &chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 4})
	c.Assert(inverse.After, qt.DeepEquals, record.Value.(*chdiff.Index).Before.Desired())
	c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.IndexKind, Value: inverse.Before}})
	c.Assert(result[0].Strategy, qt.Equals, "replace the index with its captured definition")
	c.Assert(result[0].Limitations, qt.DeepEquals, []string{"Replacing a skipping index restores its definition but does not restore materialized index data for existing table parts."})
	roundTrip, err := runtime.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{result[0].Change}})
	c.Assert(err, qt.IsNil)
	c.Assert(roundTrip[0].Change, qt.DeepEquals, record)
	inverse.Before.Granularity = 9
	inverse.After.IndexType.Value = "mutated"
	c.Assert(result[0].ForwardState[0].Value.(*chschema.ObservedIndex).Granularity, qt.Equals, uint64(4))
	c.Assert(record, qt.DeepEquals, indexChange())
}

func TestIndexReversalRejectsInvalidBatchesAtomically(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*schemaext.ReversalRequest)
		want error
	}{
		{"target", func(r *schemaext.ReversalRequest) { r.Target = "postgres" }, ptaherr.ErrUnsupportedDialect},
		{"wrong model", func(r *schemaext.ReversalRequest) { r.Changes[1].Value = &chdiff.Table{} }, schemaext.ErrInvalidValue},
		{"missing operand", func(r *schemaext.ReversalRequest) { r.Changes[1].Value.(*chdiff.Index).Before = nil }, schemaext.ErrInvalidValue},
		{"unresolved intent", func(r *schemaext.ReversalRequest) {
			r.Changes[1].Value.(*chdiff.Index).After = &chschema.DesiredIndex{}
		}, schemaext.ErrInvalidValue},
		{"wrong subject", func(r *schemaext.ReversalRequest) { r.Changes[1].Subject.Kind = objectidentity.KindTable }, schemaext.ErrInvalidValue},
		{"missing parent", func(r *schemaext.ReversalRequest) { r.Changes[1].Subject.Parent = objectidentity.Part{} }, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{indexChange(), indexChange()}}
			test.edit(&request)
			result, err := (chreverse.IndexService{}).ReverseChanges(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestIndexReversalHonorsCancellationAndRequiresContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (chreverse.IndexService{}).ReverseChanges(ctx, schemaext.ReversalRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	var missingContext context.Context
	result, err = (chreverse.IndexService{}).ReverseChanges(missingContext, schemaext.ReversalRequest{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
