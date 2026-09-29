package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRender_IndexInvisible writes an index hidden from the optimizer, and
// reports that the document leaves the visibility out: the pinned community
// binary v1.3.0 has no attribute for it, and its own `schema inspect` writes
// such an index as a visible one without a word (stokaro/ptah#3853).
func TestRender_IndexInvisible(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "INT", Primary: true},
			{StructName: "Order", Name: "total", Type: "INT"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Order", Name: "k_total", TableName: "orders", Fields: []string{"total"},
			Comment: "lookup", Invisible: true, KeyBlockSize: 8,
		}},
	}

	result, err := atlashclrender.RenderInspected(db, "mysql", "app")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `index "k_total"`)
	c.Assert(string(result.Data), qt.Contains, `comment = "lookup"`)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{
		Severity: atlashclrender.SeverityWarning,
		Path:     `indexes["orders"]["k_total"]`,
		Message:  "the index KEY_BLOCK_SIZE hint cannot be represented in HCL schema output; applying this HCL removes the hint",
	}, {
		Severity: atlashclrender.SeverityWarning,
		Path:     `indexes["orders"]["k_total"]`,
		Message:  "the index is hidden from the optimizer, which HCL schema output cannot represent; applying this HCL makes the index visible",
	}})
}
