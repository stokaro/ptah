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

// familyTable is a YDB table declaring the column families given, as the YDB
// owner's facet, or none.
func familyTable(families ...ydbschema.ColumnFamily) *schemamodel.Database {
	var facets schemaext.Facets
	if len(families) > 0 {
		facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Facets: facets}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Events", Type: "Int64", Primary: true},
			{Name: "body", StructName: "Events", Type: "Utf8", Nullable: true},
		},
	}
}

// TestRender_ReportsTheColumnFamiliesItLeavesOut names each table whose column
// families the document leaves out as a loss, which is what stops an
// annotation cleanup from deleting them. A table declaring none, and one
// holding only the default family with the settings YDB gives a family stating
// none, as a read finds a table nobody gave families, lose nothing. A default
// family a table profile compressed is named.
func TestRender_ReportsTheColumnFamiliesItLeavesOut(t *testing.T) {
	cold := ydbschema.ColumnFamily{Name: "cold", Compression: "lz4", Columns: []string{"body"}}
	tests := []struct {
		name string
		db   *schemamodel.Database
		want []atlashclrender.Diagnostic
	}{
		{name: "two families", db: familyTable(cold, ydbschema.ColumnFamily{Name: "default", Data: "ssd"}),
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.events",
				Message: "column families cold, default are not represented in HCL"}}},
		{name: "one family", db: familyTable(cold), want: []atlashclrender.Diagnostic{{
			Severity: atlashclrender.SeverityWarning, Path: "table.events",
			Message: "column family cold is not represented in HCL"}}},
		{name: "the default family as YDB has it", db: familyTable(ydbschema.ColumnFamily{Name: "default", Compression: "off"})},
		{name: "the default family a profile compressed", db: familyTable(ydbschema.ColumnFamily{Name: "default", Compression: "lz4"}),
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.events",
				Message: "column family default is not represented in HCL"}}},
		{name: "none", db: familyTable()},
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
