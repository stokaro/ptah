package mysqlreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlreport"
	"ptah.run/dialect/mysql/mysqlschema"
)

func TestColumnService_CountsEachStatedSetting(t *testing.T) {
	c := qt.New(t)

	reports, err := mysqlreport.ColumnService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{
			&mysqlschema.ObservedColumnSettings{Charset: "utf8mb4"},
			&mysqlschema.ObservedColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"},
		}})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: mysqlschema.ColumnSettingsKind, Counts: []schemaext.MetricCount{{Name: mysqlreport.ColumnCharsetMetric, Value: 1}}},
		{Kind: mysqlschema.ColumnSettingsKind, Counts: []schemaext.MetricCount{
			{Name: mysqlreport.ColumnCharsetMetric, Value: 1}, {Name: mysqlreport.ColumnOnUpdateMetric, Value: 1}}},
	})
}

func TestColumnService_RefusesTheOtherRepresentation(t *testing.T) {
	c := qt.New(t)

	reports, err := mysqlreport.ColumnService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Desired, Values: []schemaext.Value{&mysqlschema.ObservedColumnSettings{Charset: "utf8mb4"}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*MySQL column settings report has mismatched value .*`)
	c.Assert(reports, qt.IsNil)
}
