package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheExternalObjectsItLeavesOut names each YDB external data
// source and external table the document leaves out as a loss.
func TestRender_ReportsTheExternalObjectsItLeavesOut(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Events"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
		ExternalDataSources: []schemamodel.ExternalDataSource{
			{Name: "s3", Schema: "ext", SourceType: "ObjectStorage", AuthMethod: "NONE"},
		},
		ExternalTables: []schemamodel.ExternalTable{{Name: "files", DataSource: "ext/s3", Location: "f/"}},
	}

	result, err := atlashclrender.RenderForDialect(db, platform.YDB)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{Severity: atlashclrender.SeverityWarning, Path: "external_data_source.ext.s3",
			Message: "external data source ext.s3 is not represented in HCL"},
		{Severity: atlashclrender.SeverityWarning, Path: "external_table.files",
			Message: "external table files is not represented in HCL"},
	})
	c.Assert(string(result.Data), qt.Not(qt.Contains), "ObjectStorage")
}
