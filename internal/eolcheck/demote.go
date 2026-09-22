package eolcheck

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/capabilityprobe"
)

// CellsFile is the path the declarations live at, relative to the repository
// root. DemoteCell names it in its errors, so a caller that read the bytes
// from somewhere else still gets a message naming the file to open.
const CellsFile = "internal/capabilityprobe/cells.go"

// DemoteCell lowers one cell to the promise an end-of-life line can carry and
// returns the rewritten file. The source must be the cells.go this binary was
// built from; DemoteCell refuses rather than rewriting when the two disagree.
//
// It makes one change written as two fields. The declared support level drops
// from [capability.Certified] to [capability.BestEffort], and reason joins the
// cell as Unprobed, which is what stops a probe job being spent on the line.
// The two belong together: the census in capabilityprobe refuses a line that
// claims testing nothing exercises, and refuses one exercised that claims
// none.
//
// Nothing is removed. The cell keeps its capability preset, its image and its
// place in the support matrix, because upstream end of life lowers what Ptah
// promises about a release line and is not a reason to stop working with a
// server -- [capability.SupportLevel] carries that contract. What the reader
// of the matrix loses is the guarantee, not the dialect: a best-effort line is
// resolved and operated exactly as any other, and may break as the code around
// it moves, which is the difference the level exists to state.
//
// The literal is addressed by its position in the slice rather than by
// matching text. A cell spans several lines and writes its fields in whatever
// order reads best, so any text match would be a guess; the index is exact,
// because the i-th element of the slice literal is capabilityprobe.Cells[i].
// The element count is compared against the declared slice first, so a file
// that stopped being one flat literal is refused instead of mangled.
func DemoteCell(source []byte, id, reason string) ([]byte, error) {
	cell, index, found := declaredCell(id)
	if !found {
		return nil, fmt.Errorf("no declared cell has id %q", id)
	}
	if reason == "" {
		return nil, fmt.Errorf("cell %q needs a reason; an unprobed line with no reason reads as an oversight", id)
	}
	if cell.Support != capability.Certified {
		return nil, fmt.Errorf(
			"cell %q declares %s, and %s is the only level lowered here",
			id, cell.Support, capability.Certified)
	}
	// A compiled-in engine has no container to withhold, so writing Unprobed
	// on it would claim a line stopped being exercised while the probe still
	// opens it in memory.
	if cell.CompiledIn {
		return nil, fmt.Errorf("cell %q compiles its engine in, so no container can be withheld from it", id)
	}
	if cell.Unprobed != "" {
		return nil, fmt.Errorf("cell %q already declares why it is unprobed: %s", id, cell.Unprobed)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, CellsFile, source, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", CellsFile, err)
	}
	elements, err := cellElements(file)
	if err != nil {
		return nil, err
	}
	if len(elements) != len(capabilityprobe.Cells) {
		return nil, fmt.Errorf(
			"%s declares %d cell literals and this binary was built from %d; "+
				"the file is not the one these ids address, so nothing was rewritten",
			CellsFile, len(elements), len(capabilityprobe.Cells))
	}
	support, err := supportValue(elements[index])
	if err != nil {
		return nil, fmt.Errorf("cell %q in %s: %w", id, CellsFile, err)
	}
	start := fset.Position(support.Pos()).Offset
	end := fset.Position(support.End()).Offset
	// The declared level and the expression the file writes are two statements
	// about one cell, and only the first has been read so far. A cell reaching
	// capability.Certified through a name this does not recognize is refused,
	// because replacing those bytes would overwrite an expression nobody read.
	if string(source[start:end]) != certifiedExpression {
		return nil, fmt.Errorf(
			"cell %q in %s writes its support level as %s rather than %s, so nothing was rewritten",
			id, CellsFile, source[start:end], certifiedExpression)
	}
	rewritten := bytes.Clone(source[:start])
	rewritten = append(rewritten, bestEffortExpression...)
	rewritten = append(rewritten, source[end:]...)

	// The Unprobed field goes on its own line after the one the support level
	// sits on, because a cell writes several fields per line and inserting
	// mid-line would put it inside somebody else's. gofmt then re-aligns the
	// literal, which is how a cell that aligns its keys keeps doing so.
	insert := lineAfter(rewritten, end+len(bestEffortExpression)-len(certifiedExpression))
	field := "Unprobed: " + strconv.Quote(reason) + ",\n"
	with := bytes.Clone(rewritten[:insert])
	with = append(with, field...)
	with = append(with, rewritten[insert:]...)
	formatted, err := format.Source(with)
	if err != nil {
		return nil, fmt.Errorf("the rewritten %s does not format: %w", CellsFile, err)
	}
	return formatted, nil
}

// The expressions cells.go writes for the two levels. They are spelled out
// rather than derived from the constants, because what is replaced is source
// text and a constant's VALUE ("certified") is not what the file says.
const (
	certifiedExpression  = "capability.Certified"
	bestEffortExpression = "capability.BestEffort"
)

// declaredCell finds a cell and its position in the declared slice.
func declaredCell(id string) (capabilityprobe.Cell, int, bool) {
	for i, cell := range capabilityprobe.Cells {
		if capabilityprobe.CellID(cell) == id {
			return cell, i, true
		}
	}
	return capabilityprobe.Cell{}, 0, false
}

// cellElements returns the elements of the `var Cells = []Cell{...}` literal.
func cellElements(file *ast.File) ([]ast.Expr, error) {
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || value.Names[0].Name != "Cells" || len(value.Values) != 1 {
				continue
			}
			literal, ok := value.Values[0].(*ast.CompositeLit)
			if !ok {
				return nil, fmt.Errorf("Cells in %s is not a composite literal", CellsFile)
			}
			return literal.Elts, nil
		}
	}
	return nil, fmt.Errorf("%s declares no var Cells", CellsFile)
}

// supportValue is the expression one cell literal assigns to Support.
func supportValue(element ast.Expr) (ast.Expr, error) {
	literal, ok := element.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("the element is not a composite literal")
	}
	for _, field := range literal.Elts {
		pair, ok := field.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok || key.Name != "Support" {
			continue
		}
		return pair.Value, nil
	}
	return nil, fmt.Errorf("the literal names no Support field")
}

// lineAfter is the offset of the first byte past the line offset sits on.
func lineAfter(source []byte, offset int) int {
	next := bytes.IndexByte(source[offset:], '\n')
	if next < 0 {
		return len(source)
	}
	return offset + next + 1
}
