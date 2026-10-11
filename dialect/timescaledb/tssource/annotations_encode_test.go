package tssource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/timescaledb/tssource"
)

func hypertableFacets(hypertable *tsschema.DesiredHypertable) schemaext.Facets {
	return must.Must(schemaext.NewFacets(hypertable))
}

func TestEncodeAnnotations_HappyPath(t *testing.T) {
	readings := annotation.Table{Schema: "metrics", Name: "readings", Struct: "Reading"}
	aggregate := tsschema.DesiredContinuousAggregateObject("metrics", "hourly",
		tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(false), Comment: "hourly"})
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		want    []annotation.EncodedDirective
	}{
		{name: "a hypertable with every setting", request: annotation.EncodeRequest{Tables: annotation.Tables{readings}, Slots: []annotation.SlotFacets{{
			Slot:   annotation.Slot{Directive: "ptah:schema:table", Table: readings},
			Facets: hypertableFacets(&tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"}),
		}}}, want: []annotation.EncodedDirective{{Directive: tssource.HypertableDirective, Placement: annotation.AfterTable, Table: &readings,
			Attributes: []annotation.EncodedAttribute{{Name: "table", Value: "metrics.readings"}, {Name: "column", Value: "time"},
				{Name: "chunk_interval", Value: "1 day"}, {Name: "if_not_exists", Value: "true"}, {Name: "comment", Value: "by time"}}}}},
		{name: "a hypertable that sets its column alone", request: annotation.EncodeRequest{Tables: annotation.Tables{readings}, Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: "ptah:schema:table", Table: readings}, Facets: hypertableFacets(&tsschema.DesiredHypertable{Column: "time"}),
		}}}, want: []annotation.EncodedDirective{{Directive: tssource.HypertableDirective, Placement: annotation.AfterTable, Table: &readings,
			Attributes: []annotation.EncodedAttribute{{Name: "table", Value: "metrics.readings"}, {Name: "column", Value: "time"}}}}},
		{name: "an aggregate with every setting", request: annotation.EncodeRequest{Objects: []schemaext.Object{aggregate}},
			want: []annotation.EncodedDirective{{Directive: tssource.ContinuousAggregateDirective, Placement: annotation.AfterPrivileges,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "hourly"}, {Name: "schema", Value: "metrics"},
					{Name: "body", Value: "SELECT 1"}, {Name: "materialized_only", Value: "false"}, {Name: "comment", Value: "hourly"}}}}},
		{name: "an aggregate in the default schema", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			tsschema.DesiredContinuousAggregateObject("", "daily", tsschema.DesiredContinuousAggregate{Body: "SELECT 2"})}},
			want: []annotation.EncodedDirective{{Directive: tssource.ContinuousAggregateDirective, Placement: annotation.AfterPrivileges,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "daily"}, {Name: "body", Value: "SELECT 2"}}}}},
		{name: "nothing of the owner's", request: annotation.EncodeRequest{Coverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tssource.Annotations().Encoder.EncodeAnnotations(c.Context(), test.request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Directives, qt.DeepEquals, test.want)
		})
	}
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	readings := annotation.Table{Name: "readings", Struct: "Reading"}
	complete := must.Must(tsschema.CompleteCoverage(schemaext.Desired))
	limited := must.Must(schemaext.NewCoverage(schemaext.Desired, complete.KindRecords(), []schemaext.SubjectCoverage{{
		Kind: tsschema.HypertableKind, Subject: objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "devices"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "no dimension"},
	}}))
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		wantIs  error
		wantErr string
	}{
		{name: "a hypertable on an index", request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: "ptah:schema:index", Table: readings, Name: "i"}, Facets: hypertableFacets(&tsschema.DesiredHypertable{Column: "time"}),
		}}}, wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/timescaledb/hypertable"`},
		{name: "a hypertable on a primary key", request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: "ptah:schema:table", Table: readings, PrimaryKey: true}, Facets: hypertableFacets(&tsschema.DesiredHypertable{Column: "time"}),
		}}}, wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/timescaledb/hypertable"`},
		{name: "a hypertable without a column", request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: "ptah:schema:table", Table: readings}, Facets: hypertableFacets(&tsschema.DesiredHypertable{}),
		}}}, wantIs: schemaext.ErrInvalidValue, wantErr: `.*a hypertable requires a column to partition on`},
		{name: "an aggregate without a body", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			tsschema.DesiredContinuousAggregateObject("", "empty", tsschema.DesiredContinuousAggregate{Body: " "})}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a continuous aggregate requires a body`},
		{name: "an aggregate that is not a declaration", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: tsschema.DesiredContinuousAggregateObject("", "observed", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}).Ref, Value: &tsschema.DesiredHypertable{Column: "time"}}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature object .*observed.* with value \*tsschema.DesiredHypertable`},
		{name: "a limit the source cannot spell", request: annotation.EncodeRequest{Coverage: limited},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*devices cannot be exported without losing its coverage record: unrepresentable no dimension`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tssource.Annotations().Encoder.EncodeAnnotations(c.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Directives, qt.IsNil)
		})
	}
}
