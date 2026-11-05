package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

func TestSchemaInspectJSON_PreservesOtherDialectsDiagnostics(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
	report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	data, err := report.MarshalJSON()

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{}`)
	c.Assert(diagnostics.String(), qt.Equals, "")
}
