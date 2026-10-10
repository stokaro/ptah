package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTablePartitioningConvertFeatures_ProjectsEachRepresentation pins the
// projections: an observation becomes a declaration naming the same settings,
// and a declaration becomes what a table created from it holds -- read over
// YDB's documented defaults, written as what differs from them, with the
// minimum its starting layout sets and no layout.
func TestTablePartitioningConvertFeatures_ProjectsEachRepresentation(t *testing.T) {
	tests := []struct {
		name string
		from schemaext.Representation
		to   schemaext.Representation
		in   schemaext.Value
		want schemaext.Value
	}{
		{name: "an observation", from: schemaext.Observed, to: schemaext.Desired,
			in:   &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 3}},
			want: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 3}}},
		{name: "a declaration with a layout", from: schemaext.Desired, to: schemaext.Observed,
			in: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
				BySize: new(true), PartitionSizeMB: 2048, UniformPartitions: 4, ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(true)}},
			want: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 4, KeyBloomFilter: new(true)}}},
		{name: "a declaration of the defaults", from: schemaext.Desired, to: schemaext.Observed,
			in:   &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 1}},
			want: &ydbschema.ObservedTablePartitioning{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := ydbconvert.TablePartitioningService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: "ydb", From: test.from, To: test.to, Values: []schemaext.Value{test.in},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

func TestTablePartitioningConvertFeatures_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ConversionRequest
		wantErr error
	}{
		{name: "another target", request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed},
			wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "a declaration YDB refuses", request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}}}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ReadReplicas: "x"}}}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid observation", request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired,
			Values: []schemaext.Value{&ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{UniformPartitions: 4}}}},
			wantErr: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := ydbconvert.TablePartitioningService{}.ConvertFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
