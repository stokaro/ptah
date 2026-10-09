package crdbconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbconvert"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// TestConvertFeatures_KeepsEverySpelling pins that conversion keeps the stored
// and the declared spelling: it is not a read of the server.
func TestConvertFeatures_KeepsEverySpelling(t *testing.T) {
	c := qt.New(t)

	stored := crdbschema.Policy{ExpireAfter: "72:00:00", RowStatsPollInterval: "10m0s", SelectBatchSize: new(int64(3))}
	declared := crdbschema.Policy{ExpireAfter: "72 hours", LabelMetrics: true}
	desired, err := crdbconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "cockroachdb", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{&crdbschema.ObservedRowTTL{Policy: stored}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(desired, qt.DeepEquals, []schemaext.Value{&crdbschema.DesiredRowTTL{Policy: stored}})
	observed, err := crdbconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "cockroachdb", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&crdbschema.DesiredRowTTL{Policy: declared}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&crdbschema.ObservedRowTTL{Policy: declared}})
}

func TestConvertFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	valid := &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ConversionRequest
		wantErr error
	}{
		{name: "another target", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed}, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "one direction", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "cockroachdb", From: schemaext.Desired, To: schemaext.Desired}, wantErr: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "cockroachdb", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{valid}}, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "cockroachdb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{JobCron: "@daily"}}}}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, request: schemaext.ConversionRequest{Target: "cockroachdb", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{valid}}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := crdbconvert.Service{}.ConvertFeatures(test.ctx, test.request)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
