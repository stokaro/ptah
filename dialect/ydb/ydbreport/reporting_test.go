package ydbreport_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestReportValues_CountsBothRepresentationsAndDisabledStreams(t *testing.T) {
	c := qt.New(t)
	spec := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true,
		Consumers: []ast.TopicConsumerSpec{{Name: "first"}, {Name: "second"}}}
	for _, test := range []struct {
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{representation: schemaext.Desired, value: &ydbschema.DesiredChangefeed{Spec: spec}},
		{representation: schemaext.Observed, value: &ydbschema.ObservedChangefeed{Spec: spec}},
	} {
		report, err := (ydbreport.Service{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
		c.Assert(err, qt.IsNil)
		c.Assert(report, qt.DeepEquals, []schemaext.ValueReport{{Kind: ydbschema.ChangefeedKind, Counts: []schemaext.MetricCount{
			{Name: "changefeeds", Value: 1}, {Name: "changefeed_consumers", Value: 2},
		}}})
	}
}

func TestReportValues_RejectsInvalidBatchesWithoutPartialReports(t *testing.T) {
	valid := &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}}
	cases := []struct {
		name  string
		value schemaext.Value
	}{
		{name: "nil"}, {name: "typed nil", value: (*ydbschema.DesiredChangefeed)(nil)},
		{name: "wrong representation", value: &ydbschema.ObservedChangefeed{Spec: valid.Spec}},
		{name: "invalid stream", value: &ydbschema.DesiredChangefeed{}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report, err := (ydbreport.Service{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired, Values: []schemaext.Value{valid, test.value}})
			c.Assert(err, qt.IsNotNil)
			c.Assert(report, qt.IsNil)
		})
	}
}

func TestReportValues_RequiresActiveContextAndSchemaRepresentation(t *testing.T) {
	c := qt.New(t)
	service := ydbreport.Service{}
	var missingContext context.Context
	report, err := service.ReportValues(missingContext, schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
	report, err = service.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Change})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err = service.ReportValues(ctx, schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.IsNil)
}
