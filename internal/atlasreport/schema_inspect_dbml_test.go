package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// excludeTable is a desired schema whose table t holds an EXCLUDE constraint,
// which DBML has no spelling for.
func excludeTable() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "integer", Primary: true},
			{StructName: "T", Name: "during", Type: "tstzrange", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Table: "t", Name: "t_during_excl", Type: "EXCLUDE"},
		},
	}
}

// TestSchemaInspectReport_MarshalDBML_ReportsWhatItLeavesOut writes what DBML
// cannot express to the diagnostics writer, and keeps it out of the document
// (stokaro/ptah#3917). Without the warning, `schema inspect --format dbml`
// read as a complete description of a database it was not.
func TestSchemaInspectReport_MarshalDBML_ReportsWhatItLeavesOut(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	report := atlasreport.NewSchemaInspectReport(
		excludeTable(), &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"}, &diagnostics,
		atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
	)

	document, err := report.MarshalDBML()

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals,
		"warning: DBML cannot express EXCLUDE constraints (1); the export leaves them out\n")
	c.Assert(document, qt.Contains, `Table "t" {`)
	c.Assert(document, qt.Not(qt.Contains), "warning")
}

// TestSchemaInspectReport_MarshalDBML_SaysNothingWhenNothingIsLeftOut is the
// control: a schema DBML can write in full leaves the diagnostics writer
// empty.
func TestSchemaInspectReport_MarshalDBML_SaysNothingWhenNothingIsLeftOut(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db := excludeTable()
	db.Constraints = nil
	report := atlasreport.NewSchemaInspectReport(
		db, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"}, &diagnostics,
		atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
	)

	document, err := report.MarshalDBML()

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals, "")
	c.Assert(document, qt.Contains, `Table "t" {`)
}
