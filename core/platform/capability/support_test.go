package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

func TestSupportLevel_Valid_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		level capability.SupportLevel
	}{
		{name: "certified", level: capability.Certified},
		{name: "legacy tested", level: capability.LegacyTested},
		{name: "best effort", level: capability.BestEffort},
		{name: "known incompatible", level: capability.KnownIncompatible},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.level.Valid(), qt.IsTrue)
			c.Assert(test.level.Doc(), qt.Not(qt.Equals), "")
			c.Assert(test.level.String(), qt.Equals, string(test.level))
		})
	}
}

// TestSupportLevel_Valid_FailurePath pins the zero value as invalid. A release
// line whose level was never assigned must not read as one Ptah promises
// something about, and "" is what an unassigned field holds.
func TestSupportLevel_Valid_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		level capability.SupportLevel
	}{
		{name: "zero value", level: capability.SupportLevel("")},
		{name: "unknown word", level: capability.SupportLevel("supported")},
		{name: "wrong case", level: capability.SupportLevel("Certified")},
		{name: "underscored spelling", level: capability.SupportLevel("legacy_tested")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.level.Valid(), qt.IsFalse)
			c.Assert(test.level.Doc(), qt.Equals, "")
		})
	}
}

// TestSupportLevels_IsTheClosedSet keeps the enumeration and the predicate in
// step in both directions: a level added to one without the other would either
// be undocumented in the listing or reported invalid while being listed.
func TestSupportLevels_IsTheClosedSet(t *testing.T) {
	c := qt.New(t)

	levels := capability.SupportLevels()

	c.Assert(levels, qt.HasLen, 4)
	c.Assert(levels, qt.DeepEquals, []capability.SupportLevel{
		capability.Certified,
		capability.LegacyTested,
		capability.BestEffort,
		capability.KnownIncompatible,
	})
	for _, level := range levels {
		c.Assert(level.Valid(), qt.IsTrue)
		c.Assert(level.Doc(), qt.Not(qt.Equals), "")
	}
}

// TestSupportLevels_ReturnsACopy matters because the slice is package state and
// the accessor hands it to command output and to the documentation generator.
// A caller sorting or truncating the result would otherwise reorder the levels
// for every later reader.
func TestSupportLevels_ReturnsACopy(t *testing.T) {
	c := qt.New(t)

	first := capability.SupportLevels()
	first[0] = capability.SupportLevel("clobbered")

	c.Assert(capability.SupportLevels()[0], qt.Equals, capability.Certified)
}
