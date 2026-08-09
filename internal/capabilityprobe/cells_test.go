package capabilityprobe_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/capabilityprobe"
)

func TestCells_IsNotEmpty(t *testing.T) {
	c := qt.New(t)

	// A matrix with no cells covers no line, and every run against it would
	// report a line the matrix does not know. Asserting the list is populated
	// is the same refusal the repository's shell gates make when their input
	// list comes back empty.
	c.Assert(len(capabilityprobe.Cells) > 0, qt.IsTrue)
}

func TestCells_AreWellFormed(t *testing.T) {
	c := qt.New(t)

	seen := map[string]bool{}
	for _, cell := range capabilityprobe.Cells {
		c.Run(cell.String(), func(c *qt.C) {
			c.Assert(platform.NormalizeDialect(cell.Dialect), qt.Equals, cell.Dialect,
				qt.Commentf("a cell dialect must already be normalized or CellFor will never match it"))
			c.Assert(cell.Line, qt.Not(qt.Equals), "")

			key := cell.Dialect + "/" + cell.Line
			c.Assert(seen[key], qt.IsFalse, qt.Commentf("two cells claim %s; the first one found would always win", key))
			seen[key] = true

			c.Assert(cell.Refinement, qt.Not(qt.Equals), capabilityprobe.Refinement(""))
		})
	}
}

// TestCells_MeasuredCellsNameAValidPreset checks the half of a cell a reader
// cannot verify by eye: that PresetName describes the set Preset returns, and
// that the set is one the registry accepts.
func TestCells_MeasuredCellsNameAValidPreset(t *testing.T) {
	c := qt.New(t)

	named := map[string]func() capability.Capabilities{
		"Postgres17":      capability.Postgres17,
		"Postgres16":      capability.Postgres16,
		"Postgres13":      capability.Postgres13,
		"MySQL84":         capability.MySQL84,
		"MariaDB1011":     capability.MariaDB1011,
		"ClickHouse24":    capability.ClickHouse24,
		"SQLite3":         capability.SQLite3,
		"SQLServer2022":   capability.SQLServer2022,
		"CockroachDB23":   capability.CockroachDB23,
		"YugabyteDB25":    capability.YugabyteDB25,
		"SpannerPostgres": capability.SpannerPostgres,
	}
	for _, cell := range capabilityprobe.Cells {
		c.Run(cell.String(), func(c *qt.C) {
			c.Assert(cell.Measured(), qt.Equals, cell.PresetName != "",
				qt.Commentf("a cell either names a preset and has one, or names neither"))
			for _, check := range measuredChecks(cell, named) {
				check(c)
			}
		})
	}
}

// measuredChecks returns the assertions that only apply to a cell that names a
// preset, so the loop body above stays free of a conditional.
func measuredChecks(cell capabilityprobe.Cell, named map[string]func() capability.Capabilities) []func(*qt.C) {
	if !cell.Measured() {
		return nil
	}
	return []func(*qt.C){
		func(c *qt.C) {
			build, known := named[cell.PresetName]
			c.Assert(known, qt.IsTrue, qt.Commentf("cell names preset %q, which this test does not know", cell.PresetName))
			c.Assert(cell.Preset(), qt.DeepEquals, build(),
				qt.Commentf("cell names preset %q but carries a different set", cell.PresetName))
			c.Assert(cell.Preset().Validate(), qt.IsNil)
		},
	}
}

func TestCellFor(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name    string
		dialect string
		version string
		assert  func(c *qt.C, cell capabilityprobe.Cell, found bool)
	}{{
		name: "a PostgreSQL 17 patch release lands on the 17 line", dialect: platform.Postgres, version: "17.10",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Line, qt.Equals, "17")
			c.Assert(cell.PresetName, qt.Equals, "Postgres17")
		},
	}, {
		name: "a MySQL LTS line is matched on major and minor", dialect: platform.MySQL, version: "9.7.1",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Line, qt.Equals, "9.7")
		},
	}, {
		name: "a MySQL release on no LTS line matches nothing", dialect: platform.MySQL, version: "9.6.1",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
			c.Assert(cell.Line, qt.Equals, "")
		},
	}, {
		// The Postgres13 preset's own doc covers PostgreSQL 12 and 13, and the
		// matrix declares only 13 because only 13 was probed. A 12 server must
		// therefore fall off the matrix rather than borrow the 13 cell's
		// result, or the matrix certifies a line nobody measured.
		name: "a PostgreSQL major the matrix does not declare matches nothing", dialect: platform.Postgres, version: "12.22",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}, {
		name:    "SQL Server matches on the product version, not the marketing year",
		dialect: platform.SQLServer, version: "17.0.4065.4",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.Label, qt.Equals, "SQL Server 2025")
		},
	}, {
		name: "the marketing year matches no SQL Server cell", dialect: platform.SQLServer, version: "2025",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}, {
		name: "a dialect with no cells matches nothing", dialect: "nonsense", version: "1.2.3",
		assert: func(c *qt.C, cell capabilityprobe.Cell, found bool) {
			c.Assert(found, qt.IsFalse)
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)
			cell, found := capabilityprobe.CellFor(tc.dialect, version)
			tc.assert(c, cell, found)
		})
	}
}

// TestCells_CoverEveryLineTheRepositoryRuns keeps the matrix honest about the
// containers this repository actually starts: a line CI runs and the matrix
// does not declare is a line whose preset nothing measures.
func TestCells_CoverEveryLineTheRepositoryRuns(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		dialect string
		version string
	}{
		{platform.Postgres, "18.4"},
		{platform.Postgres, "17.10"},
		{platform.MySQL, "9.7.1"},
		{platform.MariaDB, "10.11.18"},
		{platform.MariaDB, "11.4.12"},
		{platform.MariaDB, "12.3.2"},
		{platform.CockroachDB, "26.2.5"},
		{platform.YugabyteDB, "2026.1.0.0"},
		{platform.SQLServer, "17.0.4065.4"},
	} {
		c.Run(fmt.Sprintf("%s %s", tc.dialect, tc.version), func(c *qt.C) {
			version, err := capabilityprobe.ParseVersion(tc.dialect, tc.version, "")
			c.Assert(err, qt.IsNil)
			_, found := capabilityprobe.CellFor(tc.dialect, version)
			c.Assert(found, qt.IsTrue)
		})
	}
}
