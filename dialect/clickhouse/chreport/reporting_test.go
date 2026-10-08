package chreport_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chschema"
)

func TestReportsCapturedSettingsInBothRepresentations(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{"declared defaults", schemaext.Desired, &chschema.DesiredTable{}},
		{"observed empty keys", schemaext.Observed, &chschema.ObservedTable{Engine: "MergeTree"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report, err := (chreport.Service{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(report, qt.DeepEquals, []schemaext.ValueReport{{Kind: chschema.TableKind, Counts: []schemaext.MetricCount{{Name: "clickhouse_table_settings", Value: 1}}}})
		})
	}
}

func TestReportsRefuseInvalidOrMismatchedValuesAtomically(t *testing.T) {
	for _, test := range []struct {
		name  string
		value schemaext.Value
	}{
		{"wrong representation", &chschema.ObservedTable{Engine: "MergeTree"}},
		{"typed nil", (*chschema.DesiredTable)(nil)},
		{"invalid intent", &chschema.DesiredTable{Engine: chschema.Setting{State: chschema.Default, Value: "MergeTree"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report, err := (chreport.Service{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired, Values: []schemaext.Value{&chschema.DesiredTable{}, test.value}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(report, qt.IsNil)
		})
	}
}

func TestReportsHonorCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := (chreport.Service{}).ReportValues(ctx, schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.IsNil)
}
