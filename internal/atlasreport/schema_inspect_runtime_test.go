package atlasreport_test

import (
	"io"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/builtintest"
)

func inspectRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return builtintest.Runtime()
}

func newInspectReport(c *qt.C, db *schemamodel.Database, schema *catalog.Database, info catalog.ServerInfo, diagnostics io.Writer, opts atlasreport.SchemaInspectReportOptions) *atlasreport.SchemaInspectReport {
	c.Helper()
	report, err := atlasreport.NewSchemaInspectReport(c.Context(), db, schema, info, diagnostics, opts, inspectRuntime(c))
	c.Assert(err, qt.IsNil)
	return report
}
