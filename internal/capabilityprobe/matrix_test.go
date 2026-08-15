package capabilityprobe_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capabilityprobe"
)

// TestCIMatrix_AccountsForEveryDeclaredLine is the census the tiered pipeline
// stands on: every declared release line either runs or is skipped for a
// stated reason, and none is quietly absent from both halves.
func TestCIMatrix_AccountsForEveryDeclaredLine(t *testing.T) {
	c := qt.New(t)

	matrix := capabilityprobe.CIMatrix()

	c.Assert(matrix.Validate(), qt.IsNil)
	c.Assert(matrix.Declared, qt.Equals, len(capabilityprobe.Cells))
	c.Assert(len(matrix.Cells)+len(matrix.Skipped), qt.Equals, matrix.Declared)
	c.Assert(len(matrix.Cells) > 0, qt.IsTrue,
		qt.Commentf("a fan-out with no cells produces no jobs and passes by examining nothing"))

	placed := map[string]int{}
	for _, cell := range slices.Concat(matrix.Cells, matrix.Skipped) {
		placed[cell.ID]++
	}
	for _, declared := range capabilityprobe.Cells {
		c.Check(placed[capabilityprobe.CellID(declared)], qt.Equals, 1,
			qt.Commentf("declared line %s appears %d times in the CI matrix",
				declared, placed[capabilityprobe.CellID(declared)]))
	}
}

// TestCIMatrix_RunnableCellsCarryEverythingOneJobNeeds checks the half a
// workflow cannot check for itself. A cell missing its URL or its container
// arguments does not fail the YAML; it fails at 03:00 in one job of eighteen.
func TestCIMatrix_RunnableCellsCarryEverythingOneJobNeeds(t *testing.T) {
	for _, cell := range capabilityprobe.CIMatrix().Cells {
		t.Run(cell.ID, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(cell.Skip, qt.Equals, "")
			c.Assert(cell.Image, qt.Not(qt.Equals), "")
			c.Assert(cell.URL, qt.Not(qt.Equals), "")
			c.Assert(cell.SuiteDatabase, qt.Not(qt.Equals), "",
				qt.Commentf("tier 3 runs the integration suite for this cell and needs the runner's own name for it"))
			c.Assert(cell.SuiteURLEnv, qt.Not(qt.Equals), "")
			c.Assert(cell.SuiteURL, qt.Not(qt.Equals), "")
			c.Assert(cell.DockerRun, qt.Contains, cell.Image,
				qt.Commentf("the docker run arguments must start the image this line declares"))
			c.Assert(strings.HasPrefix(cell.URL, cell.Dialect+"://"), qt.IsTrue,
				qt.Commentf("cell %s probes %s, which resolves to a different dialect", cell.ID, cell.URL))
		})
	}
}

// TestCIMatrix_MySQLFamilyUsesRestrictedScenarioConnections keeps the nightly
// permission-restriction scenario meaningful. The probe needs an administrator
// for capability discovery, but the suite reserves it for cleanup.
func TestCIMatrix_MySQLFamilyUsesRestrictedScenarioConnections(t *testing.T) {
	for _, id := range []string{"mysql-8-4", "mariadb-10-11"} {
		t.Run(id, func(t *testing.T) {
			c := qt.New(t)
			cell, found := capabilityprobe.CIMatrix().Find(id)
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.SuiteURL, qt.Contains, "ptah_user:ptah_password")
			c.Assert(cell.SuiteURL, qt.Not(qt.Contains), "root:root_password")
			c.Assert(cell.SuiteCleanupURLEnv, qt.Equals, strings.ToUpper(cell.Dialect)+"_CLEANUP_URL")
			c.Assert(cell.SuiteCleanupURL, qt.Contains, "root:root_password")
			c.Assert(cell.URL, qt.Equals, cell.SuiteCleanupURL,
				qt.Commentf("the capability probe and cleanup both require the administrator"))
		})
	}
}

// TestMatrix_IDs pins what the pipeline actually fans out over.
//
// The ids are the whole payload that crosses a job boundary: measured on the
// first run of the tier 2 workflow, a job output carrying the cells themselves
// is skipped by the runner because the throwaway container credentials in each
// URL look like a secret, and a strategy built from the skipped output
// produces no jobs at all. So the list must hold every runnable cell, nothing
// that cannot run, and nothing that needs quoting.
func TestMatrix_IDs(t *testing.T) {
	c := qt.New(t)

	matrix := capabilityprobe.CIMatrix()
	ids := matrix.IDs()

	c.Assert(ids, qt.HasLen, len(matrix.Cells))
	for _, cell := range matrix.Cells {
		c.Check(ids, qt.Contains, cell.ID)
	}
	for _, skipped := range matrix.Skipped {
		c.Check(ids, qt.Not(qt.Contains), skipped.ID,
			qt.Commentf("cell %s cannot run and must not appear in the fan-out", skipped.ID))
	}
	for _, id := range ids {
		c.Check(id, qt.Matches, "[a-z0-9-]+",
			qt.Commentf("an id becomes a job name, an artifact name and a file name"))
	}
}

// TestCIMatrix_SkippedCellsSayWhy keeps a skipped line from reading as a
// passing one.
func TestCIMatrix_SkippedCellsSayWhy(t *testing.T) {
	c := qt.New(t)

	skipped := capabilityprobe.CIMatrix().Skipped
	c.Assert(len(skipped) > 0, qt.IsTrue,
		qt.Commentf("ClickHouse, SQL Server, SQLite and Spanner have no probe plan or no container today; "+
			"if that changed, this test needs rewriting rather than deleting"))
	for _, cell := range skipped {
		t.Run(cell.ID, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(cell.Runnable, qt.IsFalse)
			c.Assert(cell.Skip, qt.Not(qt.Equals), "")
			c.Assert(cell.URL, qt.Equals, "")
			c.Assert(cell.DockerRun, qt.HasLen, 0)
		})
	}
}

// TestCIMatrix_MeasuredBannerLineRemainsRunnable prevents a direct live
// measurement from being hidden merely because the resolver reaches its
// preset through an engine banner rather than a version ladder.
func TestCIMatrix_MeasuredBannerLineRemainsRunnable(t *testing.T) {
	c := qt.New(t)

	cell, found := capabilityprobe.CIMatrix().Find("yugabytedb-2025-2")
	c.Assert(found, qt.IsTrue)
	c.Assert(cell.Runnable, qt.IsTrue)
	c.Assert(cell.Refinement, qt.Equals, string(capabilityprobe.RefinedByMeasuredLine))
	c.Assert(cell.Note, qt.Contains, "v2025.2.5.2-b0")
	c.Assert(cell.Note, qt.Contains, "all 25 rows match")
}

// TestCICell_TagPinsLine pins the scoping rule of stokaro/ptah#1341: the
// matrix pins a LINE and lets the registry resolve the patch.
//
// CockroachDB publishes a floating latest-v<line> alias. YugabyteDB does not,
// so its line tag is a selector the driver resolves before Docker runs. Both
// satisfy the rule; a frozen v26.2.5 or 2026.1.0.0-b118 tag would not.
func TestCICell_TagPinsLine(t *testing.T) {
	for _, tc := range []struct {
		cell string
		want bool
	}{
		{cell: "postgres-17", want: true},
		{cell: "mariadb-10-11", want: true},
		{cell: "mysql-26-7", want: true},
		{cell: "cockroachdb-26-2", want: true},
		{cell: "yugabytedb-2026-1", want: true},
	} {
		t.Run(tc.cell, func(t *testing.T) {
			c := qt.New(t)
			cell, found := capabilityprobe.CIMatrix().Find(tc.cell)
			c.Assert(found, qt.IsTrue)
			c.Assert(cell.TagPinsLine, qt.Equals, tc.want,
				qt.Commentf("image %q against line %q", cell.Image, cell.Line))
		})
	}
}

// TestMatrix_Validate covers the shapes a broken matrix takes, all of which a
// workflow would report as a successful run of nothing.
func TestMatrix_Validate(t *testing.T) {
	c := qt.New(t)

	runnable := capabilityprobe.CICell{ID: "postgres-17", Runnable: true, URL: "postgres://x", DockerRun: []string{"postgres:17"}}
	for _, tc := range []struct {
		name   string
		matrix capabilityprobe.Matrix
		assert func(c *qt.C, err error)
	}{{
		name:   "an empty matrix is refused",
		matrix: capabilityprobe.Matrix{},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, "(?s).*declares no release lines.*")
		},
	}, {
		name:   "a matrix whose every line is skipped produces no jobs",
		matrix: capabilityprobe.Matrix{Declared: 1, Skipped: []capabilityprobe.CICell{{ID: "spanner-0", Skip: "no server"}}},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, "(?s).*zero jobs.*")
		},
	}, {
		name:   "a census that does not add up is refused",
		matrix: capabilityprobe.Matrix{Declared: 9, Cells: []capabilityprobe.CICell{runnable}},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, "(?s).*census does not add up: 9 declared, 1 runnable, 0 skipped.*")
		},
	}, {
		name: "two cells with one id would collide as jobs and artifacts",
		matrix: capabilityprobe.Matrix{
			Declared: 2,
			Cells:    []capabilityprobe.CICell{runnable, runnable},
		},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, `(?s).*two cells share the id "postgres-17".*`)
		},
	}, {
		name: "a skipped cell with no reason is refused",
		matrix: capabilityprobe.Matrix{
			Declared: 2,
			Cells:    []capabilityprobe.CICell{runnable},
			Skipped:  []capabilityprobe.CICell{{ID: "spanner-0"}},
		},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, "(?s).*says no reason why.*")
		},
	}, {
		name: "a runnable cell with no server to start is refused",
		matrix: capabilityprobe.Matrix{
			Declared: 1,
			Cells:    []capabilityprobe.CICell{{ID: "postgres-17", Runnable: true, URL: "postgres://x"}},
		},
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.ErrorMatches, "(?s).*no way to start a server.*")
		},
	}, {
		name:   "the declared matrix validates",
		matrix: capabilityprobe.CIMatrix(),
		assert: func(c *qt.C, err error) {
			c.Assert(err, qt.IsNil)
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			tc.assert(c, tc.matrix.Validate())
		})
	}
}

// TestPresetGaps names every line that lacks a measured preset. The expected
// count is derived from the declaration, including the current zero-gap state,
// so adding or filling a gap cannot leave a stale hand-written count.
func TestPresetGaps(t *testing.T) {
	c := qt.New(t)

	unmeasured := slices.DeleteFunc(slices.Clone(capabilityprobe.Cells), func(cell capabilityprobe.Cell) bool {
		return cell.Measured()
	})
	c.Assert(capabilityprobe.PresetGaps(), qt.HasLen, len(unmeasured))
	for _, gap := range capabilityprobe.PresetGaps() {
		c.Check(gap, qt.ErrorMatches, "(?s)release line .* names no capability preset: .+",
			qt.Commentf("a gap must name the line and say why it is a gap"))
	}
}

// TestWriteMatrixMarkdown_CoversEveryDeclaredLine checks the generated
// documentation table against the declaration it is generated from. The
// documentation matrix is the third place the supported set is written down,
// and the owner's condition on stokaro/ptah#1341 is that all three agree.
func TestWriteMatrixMarkdown_CoversEveryDeclaredLine(t *testing.T) {
	c := qt.New(t)

	var out strings.Builder
	capabilityprobe.WriteMatrixMarkdown(&out)
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")

	c.Assert(lines, qt.HasLen, len(capabilityprobe.Cells)+2,
		qt.Commentf("one header row, one delimiter row, and one row per declared release line"))
	for _, declared := range capabilityprobe.Cells {
		c.Check(out.String(), qt.Contains, "| `"+declared.Dialect+"` | "+declared.Line+" ",
			qt.Commentf("the generated table has no row for %s", declared))
	}
	c.Assert(out.String(), qt.Not(qt.Contains), "cockroachdb/cockroach:v26.2.5",
		qt.Commentf("a frozen patch must not reappear in the generated matrix"))
	c.Assert(out.String(), qt.Contains, "| `yugabytedb/yugabyte:2025.2` | yes |",
		qt.Commentf("the directly measured YugabyteDB line must remain in the live fan-out"))
	c.Assert(out.String(), qt.Contains, "| `postgres:17` | yes |",
		qt.Commentf("and which cells satisfy it, or the column carries no information either way"))
}

// TestWriteMatrixSummary_IsTheSameMatrixNarrower keeps the site's rendering
// tied to the declaration rather than to the wide table it was cut down from.
//
// A narrow view is where a documented matrix quietly loses rows: the columns
// that did not fit are the ones a reader stops seeing, and a line dropped to
// make the table fit is a supported version nobody is told about. So the row
// count is asserted against the declaration, and every fact the dropped
// columns carried has to appear underneath the table instead.
func TestWriteMatrixSummary_IsTheSameMatrixNarrower(t *testing.T) {
	c := qt.New(t)

	var out strings.Builder
	capabilityprobe.WriteMatrixSummary(&out)
	rendered := out.String()
	table := strings.Split(rendered, "\n\nDeclared release lines:")[0]

	c.Assert(strings.Split(strings.TrimRight(table, "\n"), "\n"), qt.HasLen, len(capabilityprobe.Cells)+2,
		qt.Commentf("one header row, one delimiter row, and one row per declared release line"))

	matrix := capabilityprobe.CIMatrix()
	c.Assert(rendered, qt.Contains, fmt.Sprintf(
		"Declared release lines: %d. Probed on every pull request: %d.", matrix.Declared, len(matrix.Cells)))
	for _, cell := range matrix.Skipped {
		c.Check(rendered, qt.Contains, fmt.Sprintf("- `%s` %s — %s.", cell.Dialect, cell.Line, cell.Skip),
			qt.Commentf("the narrow table drops the reason column, so every skipped line must say why underneath"))
	}
	for _, cell := range slices.Concat(matrix.Cells, matrix.Skipped) {
		c.Check(strings.Contains(rendered, fmt.Sprintf("pinned as `%s`", cell.Image)),
			qt.Equals, cell.Image != "" && !cell.TagPinsLine,
			qt.Commentf("cell %s pins %q and its tag names the line: %t", cell.ID, cell.Image, cell.TagPinsLine))
	}
}
