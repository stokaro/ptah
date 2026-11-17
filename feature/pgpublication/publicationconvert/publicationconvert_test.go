package publicationconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationconvert"
)

// TestConvertFeatures_HappyPath pins both projections: a declaration predicts
// the observation of its effective publish set, and a read keeps its
// membership and settings but names no owner.
func TestConvertFeatures_HappyPath(t *testing.T) {
	orders := []pgpublication.Member{{Schema: "public", Table: "orders"}}
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "a declaration", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgpublication.DesiredPublication{Tables: orders, Owner: "replicator"},
			want:  &pgpublication.ObservedPublication{Tables: orders, Publish: pgpublication.PublishOperations(), Owner: "replicator"}},
		{name: "a read", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgpublication.ObservedPublication{AllTables: true, Publish: []string{"insert"}, ViaPartitionRoot: true,
				GeneratedColumns: pgpublication.GeneratedColumnsStored, Owner: "postgres"},
			want: &pgpublication.DesiredPublication{AllTables: true, Publish: []string{"insert"}, ViaPartitionRoot: true,
				GeneratedColumns: pgpublication.GeneratedColumnsStored}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := publicationconvert.Service{}.ConvertFeatures(c.Context(), schemaext.ConversionRequest{
				Target: platform.Postgres, From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

// TestConvertFeatures_FailurePath pins the requests refused.
func TestConvertFeatures_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ConversionRequest
		wantIs  error
	}{
		{name: "another target", request: schemaext.ConversionRequest{Target: platform.YugabyteDB, From: schemaext.Desired, To: schemaext.Observed},
			wantIs: ptaherr.ErrUnsupportedDialect},
		{name: "one direction", request: schemaext.ConversionRequest{Target: platform.Postgres, From: schemaext.Desired, To: schemaext.Desired},
			wantIs: schemaext.ErrInvalidValue},
		{name: "a value of the other representation", request: schemaext.ConversionRequest{Target: platform.Postgres, From: schemaext.Desired,
			To: schemaext.Observed, Values: []schemaext.Value{&pgpublication.ObservedPublication{}}}, wantIs: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := publicationconvert.Service{}.ConvertFeatures(c.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(converted, qt.IsNil)
		})
	}
}
