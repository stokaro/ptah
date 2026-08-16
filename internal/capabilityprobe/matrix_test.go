package capabilityprobe_test

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
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

// TestMatrix_ValidateRefusesABrokenMatrix covers the shapes a broken matrix
// takes, all of which a workflow would report as a successful run of nothing.
func TestMatrix_ValidateRefusesABrokenMatrix(t *testing.T) {
	runnable := capabilityprobe.CICell{ID: "postgres-17", Runnable: true, URL: "postgres://x", DockerRun: []string{"postgres:17"}}
	tests := []struct {
		name    string
		matrix  capabilityprobe.Matrix
		wantErr string
	}{{
		name:    "an empty matrix is refused",
		matrix:  capabilityprobe.Matrix{},
		wantErr: "(?s).*declares no release lines.*",
	}, {
		name:    "a matrix whose every line is skipped produces no jobs",
		matrix:  capabilityprobe.Matrix{Declared: 1, Skipped: []capabilityprobe.CICell{{ID: "spanner-0", Skip: "no server"}}},
		wantErr: "(?s).*zero jobs.*",
	}, {
		name:    "a census that does not add up is refused",
		matrix:  capabilityprobe.Matrix{Declared: 9, Cells: []capabilityprobe.CICell{runnable}},
		wantErr: "(?s).*census does not add up: 9 declared, 1 runnable, 0 skipped.*",
	}, {
		name: "two cells with one id would collide as jobs and artifacts",
		matrix: capabilityprobe.Matrix{
			Declared: 2,
			Cells:    []capabilityprobe.CICell{runnable, runnable},
		},
		wantErr: `(?s).*two cells share the id "postgres-17".*`,
	}, {
		name: "a skipped cell with no reason is refused",
		matrix: capabilityprobe.Matrix{
			Declared: 2,
			Cells:    []capabilityprobe.CICell{runnable},
			Skipped:  []capabilityprobe.CICell{{ID: "spanner-0"}},
		},
		wantErr: "(?s).*says no reason why.*",
	}, {
		name: "a runnable cell with no server to start is refused",
		matrix: capabilityprobe.Matrix{
			Declared: 1,
			Cells:    []capabilityprobe.CICell{{ID: "postgres-17", Runnable: true, URL: "postgres://x"}},
		},
		wantErr: "(?s).*no way to start a server.*",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tc.matrix.Validate(), qt.ErrorMatches, tc.wantErr)
		})
	}
}

// TestMatrix_ValidateAcceptsTheDeclaredMatrix is the control the refusals above
// need: a validator that refused everything would satisfy every row of that
// table and stop the pipeline dead.
func TestMatrix_ValidateAcceptsTheDeclaredMatrix(t *testing.T) {
	c := qt.New(t)

	c.Assert(capabilityprobe.CIMatrix().Validate(), qt.IsNil)
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

// TestWriteMatrix_BothRenderingsNameTheSupportLevelOfEveryLine ties the
// generated Support column to the declaration in both documentation views.
//
// Three failures are covered here that a reader of either page cannot see. A
// header gaining a column while the rows keep theirs renders as a table whose
// every value has shifted one place left: GFM pads the short row silently, so
// "certified" appears under "Capability preset" and the page is wrong without
// being malformed. A row gaining a cell the header lacks is worse — the excess
// is DISCARDED, taking the last column with it, which is why the cell counts
// are asserted against the header rather than against a number typed here. And
// a level that renders as an empty cell reads as a table that has always had a
// blank column, rather than as the line nobody assigned a promise to.
func TestWriteMatrix_BothRenderingsNameTheSupportLevelOfEveryLine(t *testing.T) {
	tests := []struct {
		name    string
		write   func(w io.Writer)
		columns int
	}{{
		name:    "wide",
		write:   capabilityprobe.WriteMatrixMarkdown,
		columns: 8,
	}, {
		name: "compact",
		// Five, not six: the compact rendering drops Refinement so that Support
		// fits. With both, the rendered table measured 651px inside the site's
		// 632px reading column and check-responsive.mjs failed the build.
		write:   capabilityprobe.WriteMatrixSummary,
		columns: 5,
	}}

	declared := slices.Concat(capabilityprobe.CIMatrix().Cells, capabilityprobe.CIMatrix().Skipped)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			var out strings.Builder
			tc.write(&out)
			lines := strings.Split(strings.TrimRight(strings.Split(out.String(), "\n\n")[0], "\n"), "\n")

			header := markdownCells(lines[0])
			c.Assert(header, qt.HasLen, tc.columns)
			c.Assert(header[supportColumn], qt.Equals, "Support",
				qt.Commentf("the rendered header is %q", lines[0]))
			c.Assert(markdownCells(lines[1]), qt.HasLen, len(header),
				qt.Commentf("a delimiter row shorter than the header stops the table rendering as a table"))

			rows := lines[2:]
			c.Assert(rows, qt.HasLen, len(declared))
			for i, cell := range declared {
				rendered := markdownCells(rows[i])
				c.Check(rendered, qt.HasLen, len(header),
					qt.Commentf("row %s renders %d cells against a %d-column header: %q",
						cell.ID, len(rendered), len(header), rows[i]))
				c.Check(rendered[supportColumn], qt.Equals, cell.Support.String(),
					qt.Commentf("row %s declares %q and renders %q", cell.ID, cell.Support, rendered[supportColumn]))
				c.Check(capability.SupportLevel(rendered[supportColumn]).Valid(), qt.IsTrue,
					qt.Commentf("row %s renders %q, which promises the reader nothing checkable",
						cell.ID, rendered[supportColumn]))
			}
		})
	}
}

// TestWriteMatrix_PinsALineAtEachLevel is the control the row-by-row comparison
// above needs. That test reads the declaration on both sides, so a Support
// column rendered from the wrong field — or a declaration whose levels were all
// rewritten at once — satisfies it. These three rows are the words a reader
// takes away from the page, one per level in use, and they are wrong for a
// different reason each: postgres 13 is the end-of-life line kept on purpose,
// clickhouse 26.3 is declared and exercised by nothing, and postgres 17 is the
// commitment the rest of the matrix is measured against.
func TestWriteMatrix_PinsALineAtEachLevel(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{{
		name: "a certified line",
		want: "| `postgres` | 17 | certified |",
	}, {
		name: "an end-of-life line retained as a sentinel",
		want: "| `postgres` | 13 | legacy-tested |",
	}, {
		name: "a line nothing exercises",
		want: "| `clickhouse` | 26.3 | best-effort |",
	}}

	var wide, compact strings.Builder
	capabilityprobe.WriteMatrixMarkdown(&wide)
	capabilityprobe.WriteMatrixSummary(&compact)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(wide.String(), qt.Contains, tc.want)
			c.Assert(compact.String(), qt.Contains, tc.want)
		})
	}
}

// TestWriteMatrixSummary_CountsTheDeclaredSupportLevels checks the sentence
// under the compact table against the cells it summarizes.
//
// The count is the number a reader quotes without opening cells.go, and a
// hand-written one is true on the day it is typed and false on the day a line
// moves to legacy-tested — with nothing to announce the change. So the expected
// phrases are derived here too, and the totals are compared: a level counted
// nowhere would leave the parts adding up to less than the declared lines while
// every phrase in the sentence still looked right.
func TestWriteMatrixSummary_CountsTheDeclaredSupportLevels(t *testing.T) {
	c := qt.New(t)

	counts := map[capability.SupportLevel]int{}
	for _, cell := range capabilityprobe.Cells {
		counts[cell.Support]++
	}
	levels := slices.DeleteFunc(capability.SupportLevels(), func(level capability.SupportLevel) bool {
		return counts[level] == 0
	})
	counted := 0
	phrases := make([]string, 0, len(levels))
	for _, level := range levels {
		counted += counts[level]
		phrases = append(phrases, fmt.Sprintf("%d %s", counts[level], level))
	}
	c.Assert(counted, qt.Equals, len(capabilityprobe.Cells),
		qt.Commentf("%d declared lines carry a level this vocabulary defines; the rest are counted in no phrase at all",
			counted))
	c.Assert(len(phrases) > 1, qt.IsTrue,
		qt.Commentf("every declared line sits at one level, so a sentence naming one level proves nothing about the other three"))

	var out strings.Builder
	capabilityprobe.WriteMatrixSummary(&out)

	c.Assert(out.String(), qt.Contains, fmt.Sprintf("Support levels across the %d declared lines: %s.",
		len(capabilityprobe.Cells), strings.Join(phrases, ", ")))
}

// TestCIMatrix_CellsCarryTheDeclaredSupportLevelAsJSON covers the consumer that
// never reads the Markdown: a workflow reads the fan-out as JSON, and an
// artifact published from a job is read months later by someone deciding how
// much a red cell means. The key is asserted by name because renaming it is a
// silent break — a consumer asking for `support_level` on a cell that spells it
// otherwise gets the empty string, which is a valid-looking answer meaning
// "nothing promised".
func TestCIMatrix_CellsCarryTheDeclaredSupportLevelAsJSON(t *testing.T) {
	matrix := capabilityprobe.CIMatrix()
	declared := map[string]capability.SupportLevel{}
	for _, cell := range capabilityprobe.Cells {
		declared[capabilityprobe.CellID(cell)] = cell.Support
	}

	for _, cell := range slices.Concat(matrix.Cells, matrix.Skipped) {
		t.Run(cell.ID, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(cell.Support, qt.Equals, declared[cell.ID])
			c.Assert(cell.Support.Valid(), qt.IsTrue)

			encoded, err := json.Marshal(cell)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Contains,
				fmt.Sprintf(`"support_level":%q`, cell.Support.String()))
		})
	}
}

// supportColumn is where both renderings put the promise: after the line it is
// about, before the capability preset it is independent of.
const supportColumn = 2

// markdownCells splits one rendered table row into its cells. No value the
// generator writes contains a pipe, so the split is on the column separator
// itself rather than on the character, and a row that grew or lost a cell
// changes the length this returns.
func markdownCells(row string) []string {
	return strings.Split(strings.TrimSuffix(strings.TrimPrefix(row, "| "), " |"), " | ")
}
