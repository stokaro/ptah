package eolcheck_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/eolcheck"
)

// declarationsSource is the real cells.go, which is the only input DemoteCell
// accepts: it addresses a literal by its index in capabilityprobe.Cells, so a
// hand-written fixture would describe a file those ids do not address.
func declarationsSource(c *qt.C) []byte {
	source, err := os.ReadFile(filepath.Join("..", "capabilityprobe", "cells.go"))
	c.Assert(err, qt.IsNil)
	return source
}

// cellLiteral is the text of one cell in a rewritten file, found by the image
// tag, which is the one field no two lines share.
func cellLiteral(c *qt.C, source []byte, image string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "cells.go", source, parser.ParseComments)
	c.Assert(err, qt.IsNil)
	found := ""
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || literal.Type != nil {
			return true
		}
		text := string(source[literal.Lbrace:literal.Rbrace])
		if !strings.Contains(text, `"`+image+`"`) {
			return true
		}
		found = text
		return false
	})
	c.Assert(found, qt.Not(qt.Equals), "",
		qt.Commentf("no cell in the rewritten file declares image %q", image))
	return found
}

const trialReason = "upstream support ended on 2026-08-29 (endoflife.date/clickhouse)"

// The level drops and the reason arrives together, because either alone is a
// declaration the census refuses: a line claiming testing nothing runs, or a
// line run that claims none.
func TestDemoteCell_HappyPath(t *testing.T) {
	t.Run("the level drops to best-effort", func(t *testing.T) {
		c := qt.New(t)
		source := declarationsSource(c)

		got, err := eolcheck.DemoteCell(source, "clickhouse-25-8", trialReason)

		c.Assert(err, qt.IsNil)
		literal := cellLiteral(c, got, "clickhouse/clickhouse-server:25.8")
		c.Assert(literal, qt.Contains, "capability.BestEffort")
		c.Assert(literal, qt.Not(qt.Contains), "capability.Certified")
		c.Assert(literal, qt.Contains, `Unprobed: "`+trialReason+`"`)
	})

	// Nothing is removed. The image is what a reader of the support matrix is
	// told the line ran on, and the preset is what resolves for a server on it.
	t.Run("the image and the preset stay", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.IsNil)
		literal := cellLiteral(c, got, "clickhouse/clickhouse-server:25.8")
		c.Assert(literal, qt.Contains, `Image: "clickhouse/clickhouse-server:25.8"`)
		c.Assert(literal, qt.Contains, "capability.ClickHouse2411")
	})

	// The neighbors keep their level. A rewrite that replaced every
	// capability.Certified in the file would pass every assertion above.
	t.Run("the neighboring lines are untouched", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.IsNil)
		c.Assert(cellLiteral(c, got, "clickhouse/clickhouse-server:26.3"), qt.Contains, "capability.Certified")
		c.Assert(cellLiteral(c, got, "postgres:18"), qt.Contains, "capability.Certified")
	})

	t.Run("the rewritten file is gofmt-clean", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.IsNil)
		formatted, err := format.Source(got)
		c.Assert(err, qt.IsNil)
		c.Assert(string(formatted), qt.Equals, string(got))
	})
}

func TestDemoteCell_FailurePath(t *testing.T) {
	t.Run("no cell carries the id", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "postgres-99", trialReason)
		c.Assert(err, qt.ErrorMatches, `no declared cell has id "postgres-99"`)
		c.Assert(got, qt.IsNil)
	})

	t.Run("no reason", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "clickhouse-25-8", "")
		c.Assert(err, qt.ErrorMatches,
			`cell "clickhouse-25-8" needs a reason; an unprobed line with no reason reads as an oversight`)
		c.Assert(got, qt.IsNil)
	})

	// spanner-0 already declares best-effort, so there is no promise to lower.
	t.Run("the line does not claim certification", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "spanner-0", trialReason)
		c.Assert(err, qt.ErrorMatches,
			`cell "spanner-0" declares best-effort, and certified is the only level lowered here`)
		c.Assert(got, qt.IsNil)
	})

	// sqlite-3 runs the engine compiled into the binary, so withholding a
	// container would claim it stopped being exercised while the probe still
	// opens it in memory.
	t.Run("the engine is compiled in", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(declarationsSource(c), "sqlite-3", trialReason)
		c.Assert(err, qt.ErrorMatches,
			`cell "sqlite-3" compiles its engine in, so no container can be withheld from it`)
		c.Assert(got, qt.IsNil)
	})

	// The control on the element count. A file one cell short of the
	// declarations is what a tree rewritten by something else hands in, and
	// rewriting by index there would demote whichever line moved into the slot.
	t.Run("the file does not match the declarations", func(t *testing.T) {
		c := qt.New(t)
		source := declarationsSource(c)
		short := removeLastCell(c, source)

		got, err := eolcheck.DemoteCell(short, "clickhouse-25-8", trialReason)

		c.Assert(err, qt.ErrorMatches,
			`internal/capabilityprobe/cells\.go declares \d+ cell literals and this binary `+
				`was built from \d+; the file is not the one these ids address, so nothing was rewritten`)
		c.Assert(got, qt.IsNil)
	})

	t.Run("the source does not parse", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell([]byte("package capabilityprobe\n\nvar Cells = "), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.ErrorMatches, `parse internal/capabilityprobe/cells\.go: .*`)
		c.Assert(got, qt.IsNil)
	})

	t.Run("the source declares no Cells", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell([]byte("package capabilityprobe\n"), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.ErrorMatches, `internal/capabilityprobe/cells\.go declares no var Cells`)
		c.Assert(got, qt.IsNil)
	})

	t.Run("Cells is not a literal", func(t *testing.T) {
		c := qt.New(t)
		got, err := eolcheck.DemoteCell(
			[]byte("package capabilityprobe\n\nvar Cells = declaredCells()\n"), "clickhouse-25-8", trialReason)
		c.Assert(err, qt.ErrorMatches, `Cells in internal/capabilityprobe/cells\.go is not a composite literal`)
		c.Assert(got, qt.IsNil)
	})
}

// removeLastCell drops the final element of the Cells literal, which is how a
// file that no longer describes the declared slice is produced without writing
// a second copy of cells.go into this test.
func removeLastCell(c *qt.C, source []byte) []byte {
	file, err := parser.ParseFile(token.NewFileSet(), "cells.go", source, parser.ParseComments)
	c.Assert(err, qt.IsNil)
	var last ast.Expr
	ast.Inspect(file, func(node ast.Node) bool {
		value, ok := node.(*ast.ValueSpec)
		if !ok || len(value.Names) != 1 || value.Names[0].Name != "Cells" {
			return true
		}
		literal, ok := value.Values[0].(*ast.CompositeLit)
		if !ok || len(literal.Elts) == 0 {
			return true
		}
		last = literal.Elts[len(literal.Elts)-1]
		return false
	})
	c.Assert(last, qt.IsNotNil)
	// Whole lines, so the trailing comma goes with the literal and the result
	// still parses: a file that stopped compiling would be refused for the
	// wrong reason.
	start := bytes.LastIndexByte(source[:last.Pos()-1], '\n') + 1
	end := int(last.End()-1) + bytes.IndexByte(source[last.End()-1:], '\n') + 1
	return append(bytes.Clone(source[:start]), source[end:]...)
}
