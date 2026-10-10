package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestTopicReport_CountsTopicsAndConsumers counts a declared and an observed
// topic once each, with its consumers, and refuses a value of the other
// representation.
func TestTopicReport_CountsTopicsAndConsumers(t *testing.T) {
	spec := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "b"}}}
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "desired", representation: schemaext.Desired, value: &ydbtopic.Desired{Spec: spec}},
		{name: "observed", representation: schemaext.Observed, value: &ydbtopic.Observed{Spec: spec}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.TopicService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []schemaext.ValueReport{{Kind: ydbtopic.Kind,
				Counts: []schemaext.MetricCount{{Name: "topics", Value: 1}, {Name: "topic_consumers", Value: 2}}}})
		})
	}
}

// TestTopicReport_FailurePath refuses a value of the other representation and
// a representation no topic has, with no partial report.
func TestTopicReport_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReportingRequest
	}{
		{name: "a declaration among observations", request: schemaext.ReportingRequest{Representation: schemaext.Observed,
			Values: []schemaext.Value{&ydbtopic.Observed{}, &ydbtopic.Desired{}}}},
		{name: "a change representation", request: schemaext.ReportingRequest{Representation: schemaext.Change,
			Values: []schemaext.Value{&ydbtopic.Desired{}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.TopicService{}).ReportValues(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}
