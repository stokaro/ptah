package spannerconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerconvert"
	"ptah.run/dialect/spanner/spannerschema"
)

// TestConvertFeatures_KeepsEverySpelling pins that conversion keeps the stored
// and the declared spelling: it is not a read of the server.
func TestConvertFeatures_KeepsEverySpelling(t *testing.T) {
	c := qt.New(t)

	stored := spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}
	declared := spannerschema.Policy{Column: "created_at", Interval: "30 days"}
	desired, err := spannerconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "spanner", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{&spannerschema.ObservedRowDeletion{Policy: stored}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(desired, qt.DeepEquals, []schemaext.Value{&spannerschema.DesiredRowDeletion{Policy: stored}})
	observed, err := spannerconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "spanner", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&spannerschema.DesiredRowDeletion{Policy: declared}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&spannerschema.ObservedRowDeletion{Policy: declared}})
}

func TestConvertFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	valid := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "1 days"}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ConversionRequest
		wantErr error
	}{
		{name: "another target", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed}, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "one direction", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "spanner", From: schemaext.Desired, To: schemaext.Desired}, wantErr: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "spanner", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{valid}}, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "spanner", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "36 hours"}}}}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, request: schemaext.ConversionRequest{Target: "spanner", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{valid}}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := spannerconvert.Service{}.ConvertFeatures(test.ctx, test.request)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
