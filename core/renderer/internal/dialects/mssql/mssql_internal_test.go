package mssql

// White-box testing required: these tests render AST nodes through the
// package's own Renderer, whose constructor and visitors are unexported.
// Reaching them from outside would mean going through core/renderer's dialect
// registry, which answers a different question -- that the registry routes
// here, not what this renderer emits.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// TestRenderer_ADeclaredCheckNameIsKept is the control on naming an unnamed
// column CHECK.
//
// SQL Server names an inline CHECK itself, and the comparison cannot predict
// the hash, so an unnamed one is given the convention's name. A declaration
// that named its own check must keep it: overwriting would rename a constraint
// the author chose, on every apply.
func TestRenderer_ADeclaredCheckNameIsKept(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("dbo.orders").
		AddColumn(ast.NewColumn("status", "NVARCHAR(255)").
			SetCheck("status IN ('a')").SetCheckName("chosen_name"))

	sql, err := New().Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CONSTRAINT [chosen_name] CHECK")
	c.Assert(sql, qt.Not(qt.Contains), "orders_status_check")
}

// TestRenderer_AnUnnamedCheckGetsTheConventionName is that control's own
// control: without it, a renderer that never named anything would pass the row
// above and leave the first apply disagreeing with the catalog.
func TestRenderer_AnUnnamedCheckGetsTheConventionName(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("dbo.orders").
		AddColumn(ast.NewColumn("status", "NVARCHAR(255)").SetCheck("status IN ('a')"))

	sql, err := New().Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CONSTRAINT [orders_status_check] CHECK")
}
