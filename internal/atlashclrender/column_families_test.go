package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// familyTable is a YDB table declaring the column families given.
func familyTable(families ...ast.YDBColumnFamilySpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Events", YDBColumnFamilies: families}},
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
	cold := ast.YDBColumnFamilySpec{Name: "cold", Compression: "lz4", Columns: []string{"body"}}
	tests := []struct {
		name string
		db   *schemamodel.Database
		want []atlashclrender.Diagnostic
	}{
		{name: "two families", db: familyTable(cold, ast.YDBColumnFamilySpec{Name: "default", Data: "ssd"}),
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.events",
				Message: "column families cold, default are not represented in HCL"}}},
		{name: "one family", db: familyTable(cold), want: []atlashclrender.Diagnostic{{
			Severity: atlashclrender.SeverityWarning, Path: "table.events",
			Message: "column family cold is not represented in HCL"}}},
		{name: "the default family as YDB has it", db: familyTable(ast.YDBColumnFamilySpec{Name: "default", Compression: "off"})},
		{name: "the default family a profile compressed", db: familyTable(ast.YDBColumnFamilySpec{Name: "default", Compression: "lz4"}),
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
