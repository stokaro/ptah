package chconvert_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func TestIndexConversionPreservesValuesAcrossCodecBoundary(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs:      append(chschema.Codecs(), chschema.IndexCodecs()...),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind, chschema.IndexKind}, Service: chconvert.Service{}}},
	}))
	index := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}
	values, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired,
		Values: []schemaext.Value{&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}, index},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 2)
	desired := values[1].(*chschema.DesiredIndex)
	c.Assert(desired, qt.DeepEquals, index.Desired())
	envelopes, err := runtime.Codecs().Encode(t.Context(), schemaext.Desired, []schemaext.Payload{values[0], desired})
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().Decode(t.Context(), envelopes)
	c.Assert(err, qt.IsNil)
	projected, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{decoded[0].(schemaext.Value), decoded[1].(schemaext.Value)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(projected[1], qt.DeepEquals, index)
	desired.IndexType.Value = "minmax"
	desired.Granularity.Value = 1
	c.Assert(index.IndexType, qt.Equals, "set(100)")
	c.Assert(index.Granularity, qt.Equals, uint64(math.MaxUint64))
	c.Assert(projected[1], qt.DeepEquals, index)
}

func TestIndexConversionDiscardsMixedBatchOnInvalidDeclaration(t *testing.T) {
	for _, test := range []struct {
		name  string
		value schemaext.Value
	}{
		{"unmanaged type", &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 1}}},
		{"default type", &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default}, Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 1}}},
		{"unmanaged granularity", &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Explicit, Value: "minmax"}}},
		{"default granularity", &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Explicit, Value: "minmax"}, Granularity: chschema.GranularitySetting{State: chschema.Default}}},
		{"nil declaration", (*chschema.DesiredIndex)(nil)},
		{"wrong representation", &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := (chconvert.Service{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed,
				Values: []schemaext.Value{(&chschema.ObservedTable{Engine: "Memory"}).Desired(), test.value},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestIndexConversionRefusesInvalidObservations(t *testing.T) {
	for _, value := range []*chschema.ObservedIndex{nil, {Granularity: 1}, {IndexType: "minmax"}} {
		c := qt.New(t)
		values, err := (chconvert.Service{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{
			Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired,
			Values: []schemaext.Value{&chschema.ObservedTable{Engine: "Memory"}, value},
		})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(values, qt.IsNil)
	}
}
