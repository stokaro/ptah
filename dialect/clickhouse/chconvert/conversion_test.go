package chconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func TestSelectedConversionPreservesInspectedEmptyProperties(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.Codecs(),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind}, Service: chconvert.Service{}}},
	}))
	observed := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tenant, id", PrimaryKey: "tenant"}
	values, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{observed},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	desired := values[0].(*chschema.DesiredTable)
	c.Assert(desired.PartitionBy, qt.DeepEquals, chschema.Setting{State: chschema.Explicit})
	c.Assert(desired.PrimaryKey, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: "tenant"})
	envelopes, err := runtime.Codecs().Encode(t.Context(), schemaext.Desired, []schemaext.Payload{desired})
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().Decode(t.Context(), envelopes)
	c.Assert(err, qt.IsNil)
	projected, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{decoded[0].(schemaext.Value)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(projected, qt.DeepEquals, []schemaext.Value{observed})
	desired.PrimaryKey.Value = "id"
	c.Assert(observed.PrimaryKey, qt.Equals, "tenant")
	c.Assert(projected[0].(*chschema.ObservedTable).PrimaryKey, qt.Equals, "tenant")
}

func TestConversionRefusesUnresolvedIntentAtomically(t *testing.T) {
	for _, state := range []chschema.SettingState{chschema.Unspecified, chschema.Default} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			complete := (&chschema.ObservedTable{Engine: "Memory"}).Desired()
			unresolved := complete.Clone().(*chschema.DesiredTable)
			unresolved.PrimaryKey.State = state
			values, err := chconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{complete, unresolved},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestConversionRefusesInvalidRequests(t *testing.T) {
	for _, test := range []struct {
		name    string
		request schemaext.ConversionRequest
		want    error
	}{
		{"wrong target", schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired}, ptaherr.ErrUnsupportedDialect},
		{"same representation", schemaext.ConversionRequest{Target: "clickhouse", From: schemaext.Desired, To: schemaext.Desired}, schemaext.ErrInvalidValue},
		{"missing representation", schemaext.ConversionRequest{Target: "clickhouse", To: schemaext.Desired}, schemaext.ErrInvalidValue},
		{"nil observation", schemaext.ConversionRequest{Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{(*chschema.ObservedTable)(nil)}}, schemaext.ErrInvalidValue},
		{"wrong concrete type", schemaext.ConversionRequest{Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&chschema.ObservedTable{Engine: "Memory"}}}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := chconvert.Service{}.ConvertFeatures(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestConversionHonorsContextForEmptyBatch(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := schemaext.ConversionRequest{Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired}
	values, err := chconvert.Service{}.ConvertFeatures(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(values, qt.IsNil)
	var absentContext context.Context
	values, err = chconvert.Service{}.ConvertFeatures(absentContext, request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(values, qt.IsNil)
}
