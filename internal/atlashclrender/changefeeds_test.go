package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlashclrender"
)

// changefeedTable is a YDB table carrying a changefeed, or none.
func changefeedTable(changefeeds ...ydbschema.ChangefeedSpec) *schemamodel.Database {
	var objects []schemaext.Object
	for _, stream := range changefeeds {
		objects = append(objects, ydbschema.DesiredObject("", "events", stream))
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)),
		Tables:          []schemamodel.Table{{Name: "events", StructName: "Events"}},
		Fields:          []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
	}
}

// TestRender_ReportsTheChangefeedsItLeavesOut names each table whose
// changefeeds the document leaves out as a loss, which is what stops an
// annotation cleanup from deleting them; a table carrying none is the control.
func TestRender_ReportsTheChangefeedsItLeavesOut(t *testing.T) {
	keys := ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	keysLoss := atlashclrender.Diagnostic{Severity: atlashclrender.SeverityWarning,
		Path:    `features["ptah.run/ydb/changefeed"][""][""]["events"]["keys"][""]`,
		Message: "feature object ptah.run/ydb/changefeed events.keys of kind ptah.run/ydb/changefeed is not represented in HCL"}
	updatesLoss := atlashclrender.Diagnostic{Severity: atlashclrender.SeverityWarning,
		Path:    `features["ptah.run/ydb/changefeed"][""][""]["events"]["updates"][""]`,
		Message: "feature object ptah.run/ydb/changefeed events.updates of kind ptah.run/ydb/changefeed is not represented in HCL"}
	tests := []struct {
		name string
		db   *schemamodel.Database
		want []atlashclrender.Diagnostic
	}{
		{name: "a table carrying two", db: changefeedTable(ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}, keys), want: []atlashclrender.Diagnostic{keysLoss, updatesLoss}},
		{name: "a table carrying one", db: changefeedTable(keys), want: []atlashclrender.Diagnostic{keysLoss}},
		{name: "a table carrying none", db: changefeedTable()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderForDialect(test.db, platform.YDB)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.want)
		})
	}
}
