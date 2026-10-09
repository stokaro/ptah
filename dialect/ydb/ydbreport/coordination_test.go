package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbreport"
)

func TestCoordinationReportsCountNamedObjectsWithoutTables(t *testing.T) {
	c := qt.New(t)
	result, err := (ydbreport.CoordinationService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed, Values: []schemaext.Value{&ydbcoordination.Observed{}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []schemaext.ValueReport{{Kind: ydbcoordination.Kind, Counts: []schemaext.MetricCount{{Name: "coordination_nodes", Value: 1}}}})
	result, err = (ydbreport.CoordinationService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed, Values: []schemaext.Value{&ydbcoordination.Observed{}, &ydbcoordination.Desired{}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
