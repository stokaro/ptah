package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlreport"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
)

// The MySQL owner's reports pass the runtime, which refuses a report that
// leaves one of its kind's metrics out. A column with only a character set,
// which is every text column a read reports, and an index without a hint are
// the values that exposed it.
func TestReportFeatures_MySQLObservations(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())

	report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "mysql", Representation: schemaext.Observed, Values: []schemaext.Value{
		&mysqlschema.ObservedColumnSettings{Charset: "utf8mb4"},
		&mysqlschema.ObservedIndexBlockSize{},
		&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true},
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: mysqlschema.ColumnSettingsKind, Counts: []schemaext.MetricCount{
			{Name: mysqlreport.ColumnCharsetMetric, Value: 1}, {Name: mysqlreport.ColumnOnUpdateMetric, Value: 0}}},
		{Kind: mysqlschema.IndexBlockSizeKind, Counts: []schemaext.MetricCount{{Name: mysqlreport.IndexBlockSizeMetric, Value: 0}}},
		{Kind: mysqlschema.IndexBlockSizeKind, Counts: []schemaext.MetricCount{{Name: mysqlreport.IndexBlockSizeMetric, Value: 1}}},
	})
}
