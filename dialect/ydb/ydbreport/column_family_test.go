package ydbreport_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestColumnFamiliesReportValues_CountsTheStatedFamilies counts the families a
// table states, one report per value: a default family that states nothing is
// what every row table holds and counts none, and one with a setting counts.
func TestColumnFamiliesReportValues_CountsTheStatedFamilies(t *testing.T) {
	families := []ydbschema.ColumnFamily{{Name: "default"}, {Name: "cold", Data: "hdd", Columns: []string{"body"}}, {Name: "warm"}}
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
		want           []int
	}{
		{name: "a declaration", representation: schemaext.Desired, values: []schemaext.Value{&ydbschema.DesiredColumnFamilies{Families: families}}, want: []int{2}},
		{name: "observations", representation: schemaext.Observed, values: []schemaext.Value{
			&ydbschema.ObservedColumnFamilies{Families: families},
			&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}}},
			&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}}},
		}, want: []int{2, 0, 1}},
		{name: "no value", representation: schemaext.Observed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := make([]schemaext.ValueReport, 0, len(test.want))
			for _, count := range test.want {
				want = append(want, schemaext.ValueReport{Kind: ydbschema.ColumnFamiliesKind, Counts: []schemaext.MetricCount{{Name: "ydb_column_families", Value: count}}})
			}

			reports, err := ydbreport.ColumnFamiliesService{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: test.values})

			c.Assert(err, qt.IsNil)
			c.Assert(reports, qt.DeepEquals, want)
		})
	}
}

// TestColumnFamiliesReportValues_FailurePath refuses a value of the other
// representation, an invalid value, a representation that is not a schema's
// and a canceled context, with no partial report.
func TestColumnFamiliesReportValues_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	valid := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ReportingRequest
		wantErr error
	}{
		{name: "a mismatched value", ctx: t.Context(), request: schemaext.ReportingRequest{Representation: schemaext.Observed, Values: []schemaext.Value{valid}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid value", ctx: t.Context(), request: schemaext.ReportingRequest{Representation: schemaext.Desired,
			Values: []schemaext.Value{valid, &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Compression: "zstd"}}}}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "a change representation", ctx: t.Context(), request: schemaext.ReportingRequest{Representation: schemaext.Change}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, request: schemaext.ReportingRequest{Representation: schemaext.Desired, Values: []schemaext.Value{valid}},
			wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := ydbreport.ColumnFamiliesService{}.ReportValues(test.ctx, test.request)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(reports, qt.IsNil)
		})
	}
	c := qt.New(t)
	c.Assert(ydbreport.ColumnFamiliesDefinitions(), qt.DeepEquals, []schemaext.ReportDefinition{{Kind: ydbschema.ColumnFamiliesKind, DisplayName: "column families",
		Metrics: []schemaext.MetricDefinition{{Name: "ydb_column_families", Help: "YDB column families a table states, without a default family that states nothing"}}}})
}
