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

// TestColumnFamiliesConvertFeatures_ProjectsEachRepresentation pins the
// projections: an observation becomes a declaration that keeps every family
// exactly, keep_in_memory and the default family included, and a declaration
// becomes the families a table created from it holds. Neither shares a column
// list with its input.
func TestColumnFamiliesConvertFeatures_ProjectsEachRepresentation(t *testing.T) {
	c := qt.New(t)
	families := []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}, {Name: "cold", Data: "hdd", Columns: []string{"body"}}}
	observation := &ydbschema.ObservedColumnFamilies{Families: families}
	declaration := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "warm", CacheMode: "in_memory", Columns: []string{"blob"}}}}

	desired, err := ydbconvert.ColumnFamiliesService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{observation},
	})
	c.Assert(err, qt.IsNil)
	observed, err := ydbconvert.ColumnFamiliesService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "ydb", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declaration},
	})
	c.Assert(err, qt.IsNil)
	desired[0].(*ydbschema.DesiredColumnFamilies).Families[1].Columns[0] = "mutated"
	observed[0].(*ydbschema.ObservedColumnFamilies).Families[0].Columns[0] = "mutated"

	c.Assert(desired, qt.HasLen, 1)
	c.Assert(desired[0].(*ydbschema.DesiredColumnFamilies).Families[0], qt.DeepEquals, families[0])
	c.Assert(observation.Families[1].Columns, qt.DeepEquals, []string{"body"})
	c.Assert(observed, qt.HasLen, 1)
	c.Assert(observed[0].(*ydbschema.ObservedColumnFamilies).Families[0].CacheMode, qt.Equals, "in_memory")
	c.Assert(declaration.Families[0].Columns, qt.DeepEquals, []string{"blob"})
}

func TestColumnFamiliesConvertFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	valid := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}}}
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ConversionRequest
		wantErr error
	}{
		{name: "another target", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed},
			wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "one direction", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Observed},
			wantErr: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed,
			To: schemaext.Desired, Values: []schemaext.Value{valid}}, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}, {Name: "cold"}}}}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid observation", ctx: t.Context(), request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired,
			Values: []schemaext.Value{&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Columns: []string{"a"}}}}}},
			wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
			Values: []schemaext.Value{valid}}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := ydbconvert.ColumnFamiliesService{}.ConvertFeatures(test.ctx, test.request)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
