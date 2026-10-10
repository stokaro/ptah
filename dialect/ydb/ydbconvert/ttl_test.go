package ydbconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTTLConvertFeatures_ProjectsEachRepresentation pins the projections: an
// observation becomes a declaration without its run interval, and a declaration
// becomes the TTL YDB shows of it.
func TestTTLConvertFeatures_ProjectsEachRepresentation(t *testing.T) {
	c := qt.New(t)

	desired, err := ydbconvert.TTLService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "ydb", From: schemaext.Observed, To: schemaext.Desired,
		Values: []schemaext.Value{&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}, RunIntervalSeconds: 60}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(desired, qt.DeepEquals, []schemaext.Value{&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}}})
	observed, err := ydbconvert.TTLService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT90M", Unit: "SECONDS"}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT1H30M", Unit: "SECONDS"}}})
}

func TestTTLConvertFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	valid := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ConversionRequest
		wantErr error
	}{
		{name: "another target", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "spanner", From: schemaext.Desired, To: schemaext.Observed}, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "one direction", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Desired}, wantErr: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{valid}}, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "30 days"}}}}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{valid}}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := ydbconvert.TTLService{}.ConvertFeatures(test.ctx, test.request)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
