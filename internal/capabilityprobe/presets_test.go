package capabilityprobe_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
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
		t.Run(preset, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(shipped, qt.Contains, name,
				qt.Commentf("%s names no preset in %s", name, presetSource))
			c.Check(len(reason) > 20, qt.IsTrue,
				qt.Commentf("an absence has to be argued for, and %q does not argue anything", reason))
		})
	}
}

// TestNamedPresets_ListEveryPresetPtahShips ties the exported preset list to
// the same source parse the matrix uses. Go cannot enumerate its own package's
// exported functions at run time, so capability.NamedPresets is written out --
// and a written-out list goes stale in the comfortable direction unless
// something reads the source. This is that something (stokaro/ptah#916).
func TestNamedPresets_ListEveryPresetPtahShips(t *testing.T) {
	c := qt.New(t)

	shipped := presetConstructors(c)
	listed := map[string]bool{}
	for _, preset := range capability.NamedPresets() {
		c.Check(listed[preset.Name], qt.IsFalse,
			qt.Commentf("preset %s is listed twice, so one column of the documented matrix is a duplicate",
				preset.Name))
		c.Check(preset.Capabilities.Validate(), qt.IsNil,
			qt.Commentf("preset %s is listed with a set that does not validate", preset.Name))
		listed[preset.Name] = true
	}

	for _, preset := range shipped {
		t.Run(preset, func(t *testing.T) {
			c := qt.New(t)
			c.Check(listed[preset], qt.IsTrue,
				qt.Commentf("preset %s is not in capability.NamedPresets, so the documented matrix "+
					"has no column for it", preset))
		})
	}
	c.Check(listed, qt.HasLen, len(shipped),
		qt.Commentf("NamedPresets lists %d presets and %s declares %d", len(listed), presetSource, len(shipped)))
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
