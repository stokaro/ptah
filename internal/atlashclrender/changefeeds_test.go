package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// changefeedTable is a YDB table carrying a changefeed, or none.
func changefeedTable(changefeeds ...ast.ChangefeedSpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Changefeeds: changefeeds}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
	}
}

// TestRender_ReportsTheChangefeedsItLeavesOut names each table whose
// changefeeds the document leaves out as a loss, which is what stops an
// annotation cleanup from deleting them; a table carrying none is the control.
func TestRender_ReportsTheChangefeedsItLeavesOut(t *testing.T) {
	keys := ast.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	tests := []struct {
		name string
		db   *schemamodel.Database
		want []atlashclrender.Diagnostic
	}{
		{name: "a table carrying two", db: changefeedTable(ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES",
			Format: "JSON"}, keys), want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
			Path: "table.events", Message: "changefeeds updates, keys are not represented in HCL"}}},
		{name: "a table carrying one", db: changefeedTable(keys), want: []atlashclrender.Diagnostic{{
			Severity: atlashclrender.SeverityWarning, Path: "table.events",
			Message: "changefeed keys is not represented in HCL"}}},
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
