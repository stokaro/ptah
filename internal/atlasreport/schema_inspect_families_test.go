package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasreport"
)

// An inspected YDB table carries its TTL and its column families as the YDB
// owner's facets, and every row table holds a default family. Column families
// have no HCL spelling, so the document names the ones it leaves out; that
// must not keep the TTL from becoming the table's YDB platform properties,
// which the property encoder refuses to write for any table while one facet
// it is given has no property spelling.
func TestSchemaInspectHCLExportsTheTTLBesideColumnFamilies(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(ydbTTLFacets(c).With(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "default", Compression: "off"}, {Name: "cold", Compression: "lz4", Columns: []string{"body"}},
	}}))
	facets = must.Must(facets.WithTargetScope(ydbschema.ColumnFamiliesKind, "ydb"))
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: facets}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "Int64", Primary: true},
			{Name: "created_at", StructName: "Event", Type: "Timestamp", Nullable: true},
			{Name: "body", StructName: "Event", Type: "Utf8", Nullable: true},
		},
	}
	var diagnostics strings.Builder
	report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "ydb"}, &diagnostics, atlasreport.SchemaInspectReportOptions{})

	document, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(document, qt.Contains, "  platform \"ydb\" {\n    override \"row_deletion_column\" {\n      value = \"created_at\"\n    }\n"+
		"    override \"row_deletion_interval\" {\n      value = \"P1D\"\n    }\n  }\n")
	c.Assert(diagnostics.String(), qt.Contains, "column family cold is not represented in HCL")
	c.Assert(db.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{ydbschema.ColumnFamiliesKind, ydbschema.TTLKind})
}
