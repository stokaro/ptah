package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/internal/atlasreport"
)

// TestSchemaInspectHCLWritesSQLiteTableOptionsAsAttributes writes an inspected
// STRICT or WITHOUT ROWID table as the table's strict and without_rowid
// attributes, the spelling the community binary prints and reads. Encoding the
// owner's facet as a sqlite platform block instead gave a document that binary
// refuses.
func TestSchemaInspectHCLWritesSQLiteTableOptionsAsAttributes(t *testing.T) {
	c := qt.New(t)
	declared := &sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true, WithoutRowID: true}}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "T", Facets: must.Must(schemaext.NewFacets(declared))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "T", Type: "TEXT", Primary: true}},
	}
	var diagnostics strings.Builder
	report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "sqlite"}, &diagnostics, atlasreport.SchemaInspectReportOptions{})

	document, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals, "")
	c.Assert(document, qt.Contains, "  strict = true\n")
	c.Assert(document, qt.Contains, "  without_rowid = true\n")
	c.Assert(document, qt.Not(qt.Contains), `platform "sqlite"`)
}
