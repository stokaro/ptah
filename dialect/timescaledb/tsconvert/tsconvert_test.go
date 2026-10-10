package tsconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsconvert"
	"ptah.run/dialect/timescaledb/tsschema"
)

// TestConvertFeatures_HappyPath pins both directions for both models, in
// input order.
func TestConvertFeatures_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		from, to schemaext.Representation
		values   []schemaext.Value
		want     []schemaext.Value
	}{
		{
			name: "observed to desired", from: schemaext.Observed, to: schemaext.Desired,
			values: []schemaext.Value{
				&tsschema.ObservedHypertable{Column: "time", ColumnType: "timestamptz", ChunkInterval: "7 days", Dimensions: 1},
				&tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;", MaterializedOnly: new(true), HypertableName: "readings"},
			},
			want: []schemaext.Value{
				&tsschema.DesiredHypertable{Column: "time", ChunkInterval: "7 days"},
				&tsschema.DesiredContinuousAggregate{Body: "SELECT 1;", MaterializedOnly: new(true)},
			},
		},
		{
			name: "desired to observed", from: schemaext.Desired, to: schemaext.Observed,
			values: []schemaext.Value{
				&tsschema.DesiredContinuousAggregate{Body: "SELECT 1", Comment: "c"},
				&tsschema.DesiredHypertable{Column: "time", IfNotExists: true},
			},
			want: []schemaext.Value{
				&tsschema.ObservedContinuousAggregate{Definition: "SELECT 1"},
				&tsschema.ObservedHypertable{Column: "time", Dimensions: 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := tsconvert.Service{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: test.from, To: test.to, Values: test.values})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestConvertFeatures_FailurePath pins the refusals: another target family, a
// direction that is not a conversion, a value of the wrong representation, and
// a canceled context.
func TestConvertFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	observed := []schemaext.Value{&tsschema.ObservedHypertable{Column: "time", Dimensions: 1}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ConversionRequest
		want    error
	}{
		{name: "another target", ctx: context.Background(),
			request: schemaext.ConversionRequest{Target: "mysql", From: schemaext.Observed, To: schemaext.Desired, Values: observed}, want: ptaherr.ErrUnsupportedDialect},
		{name: "the same representation", ctx: context.Background(),
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Observed, Values: observed}, want: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", ctx: context.Background(),
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: observed}, want: schemaext.ErrInvalidValue},
		{name: "an observation naming no dimension", ctx: context.Background(),
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{&tsschema.ObservedHypertable{Dimensions: 1}}}, want: schemaext.ErrInvalidValue},
		{name: "a canceled context", ctx: canceled,
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: observed}, want: context.Canceled},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := tsconvert.Service{}.ConvertFeatures(test.ctx, test.request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}
