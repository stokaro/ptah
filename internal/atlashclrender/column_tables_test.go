package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

func TestRender_ColumnStorageIsALoss(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T", Name: "events", YDBColumnTable: &ast.YDBColumnTableSpec{HashColumns: []string{"id"}, Partitions: 1}}}, Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "Uint64", Primary: true}}}
	result, err := atlashclrender.RenderInspectedForAtlasCLI(db, platform.YDB, "")
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.events", Message: "YDB column storage, hash partitioning and tiered TTL are not represented in HCL"}})
}
