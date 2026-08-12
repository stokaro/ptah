package capabilityprobe_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capabilityprobe"
)

// presetSource declares every capability preset Ptah ships.
const presetSource = "../../core/platform/capability/capability.go"

// TestCells_ClaimEveryPresetPtahShips is the direction of the drift check that
// a matrix cell cannot state for itself: a preset with no cell is a capability
// claim nothing in this repository can measure.
//
// The preset list is read out of the source rather than written down here. The
// list this test would otherwise carry is a fourth place the supported set
// lives, and a hand-written one goes stale in the comfortable direction: a
// preset added and forgotten simply never appears, and the check passes by
// examining a set that no longer matches the package.
func TestCells_ClaimEveryPresetPtahShips(t *testing.T) {
	c := qt.New(t)

	shipped := presetConstructors(c)
	c.Assert(len(shipped) > 8, qt.IsTrue,
		qt.Commentf("only %d presets were read out of %s; the parse is broken, not the matrix",
			len(shipped), presetSource))

	claimed := map[string]bool{}
	for _, cell := range capabilityprobe.Cells {
		claimed[cell.PresetName] = true
	}

	for _, preset := range shipped {
		c.Run(preset, func(c *qt.C) {
			_, excused := capabilityprobe.PresetsWithoutCell[preset]
			c.Check(claimed[preset] || excused, qt.IsTrue,
				qt.Commentf("preset %s has no matrix cell and no entry in PresetsWithoutCell, so nothing "+
					"here can measure the claim it makes", preset))
			c.Check(claimed[preset] && excused, qt.IsFalse,
				qt.Commentf("preset %s is listed as having no cell and a cell claims it; one of the two is stale", preset))
		})
	}
}

// TestPresetsWithoutCell_NameRealPresets keeps the excuse list from outliving
// what it excuses. An entry naming a preset the package no longer ships is an
// excuse for nothing, and it would go on hiding the next preset that shares
// its name.
func TestPresetsWithoutCell_NameRealPresets(t *testing.T) {
	c := qt.New(t)

	shipped := presetConstructors(c)
	c.Assert(capabilityprobe.PresetsWithoutCell, qt.Not(qt.HasLen), 0)
	for name, reason := range capabilityprobe.PresetsWithoutCell {
		c.Run(name, func(c *qt.C) {
			c.Check(shipped, qt.Contains, name,
				qt.Commentf("%s names no preset in %s", name, presetSource))
			c.Check(len(reason) > 20, qt.IsTrue,
				qt.Commentf("an absence has to be argued for, and %q does not argue anything", reason))
		})
	}
}

// presetConstructors returns the name of every exported function in the
// capability package that takes nothing and returns a Capabilities set, which
// is the shape every preset has.
func presetConstructors(c *qt.C) []string {
	c.Helper()

	parsed, err := parser.ParseFile(token.NewFileSet(), presetSource, nil, 0)
	c.Assert(err, qt.IsNil)

	var names []string
	for _, decl := range parsed.Decls {
		function, isFunction := decl.(*ast.FuncDecl)
		if !isFunction || function.Recv != nil || !function.Name.IsExported() {
			continue
		}
		if !returnsCapabilitiesOnly(function.Type) {
			continue
		}
		names = append(names, function.Name.Name)
	}
	slices.Sort(names)
	return names
}

func returnsCapabilitiesOnly(signature *ast.FuncType) bool {
	if len(signature.Params.List) != 0 || signature.Results == nil || len(signature.Results.List) != 1 {
		return false
	}
	result, named := signature.Results.List[0].Type.(*ast.Ident)
	return named && result.Name == "Capabilities"
}
