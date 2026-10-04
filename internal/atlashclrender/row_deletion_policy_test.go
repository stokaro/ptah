package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// HCL has no spelling for a row deletion policy, so the render names every
// policy it leaves out: a cleanup of the Go annotations the document was
// written from refuses rather than loses one.
func TestRender_RowDeletionPolicyIsALoss(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Events", Name: "events", Schema: "app", PrimaryKey: []string{"id"},
			RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}}},
		Fields: []schemamodel.Field{
			{StructName: "Events", Name: "id", Type: "Uint64", Primary: true},
			{StructName: "Events", Name: "ts", Type: "Timestamp", Nullable: true},
		},
	}

	result, err := atlashclrender.RenderInspectedForAtlasCLI(db, platform.YDB, "")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), "P30D")
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{
		Severity: atlashclrender.SeverityWarning,
		Path:     "table.app.events",
		Message:  "row deletion policy (TTL P30D on ts) is not represented in HCL",
	}})
}
