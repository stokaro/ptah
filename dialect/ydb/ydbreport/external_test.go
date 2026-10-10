package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreport"
)

// TestExternalReport_CountsObjectsAndColumns counts each data source and each
// external table once, and a table's columns, in either representation.
func TestExternalReport_CountsObjectsAndColumns(t *testing.T) {
	source := ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}
	table := ydbexternal.Table{DataSource: "s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "a", Type: "Int64"}, {Name: "b", Type: "Utf8"}}}
	want := []schemaext.ValueReport{
		{Kind: ydbexternal.SourceKind, Counts: []schemaext.MetricCount{{Name: "external_data_sources", Value: 1}}},
		{Kind: ydbexternal.TableKind, Counts: []schemaext.MetricCount{{Name: "external_tables", Value: 1}, {Name: "external_columns", Value: 2}}},
	}
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
	}{
		{name: "desired", representation: schemaext.Desired,
			values: []schemaext.Value{&ydbexternal.DesiredSource{Spec: source}, &ydbexternal.DesiredTable{Spec: table}}},
		{name: "observed", representation: schemaext.Observed,
			values: []schemaext.Value{&ydbexternal.ObservedSource{Spec: source}, &ydbexternal.ObservedTable{Spec: table}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.ExternalService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: test.values})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, want)
		})
	}
}

// TestExternalReport_FailurePath refuses a value of the other representation
// and a representation no external object has, with no partial report.
func TestExternalReport_FailurePath(t *testing.T) {
	source := ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}
	tests := []struct {
		name    string
		request schemaext.ReportingRequest
	}{
		{name: "a declaration among observations", request: schemaext.ReportingRequest{Representation: schemaext.Observed,
			Values: []schemaext.Value{&ydbexternal.ObservedSource{Spec: source}, &ydbexternal.DesiredSource{Spec: source}}}},
		{name: "a change representation", request: schemaext.ReportingRequest{Representation: schemaext.Change,
			Values: []schemaext.Value{&ydbexternal.DesiredSource{Spec: source}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.ExternalService{}).ReportValues(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}
