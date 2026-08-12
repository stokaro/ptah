package capmatrix

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/capabilityprobe"
)

// CellVerdict is one expected cell's line in the tier report.
type CellVerdict struct {
	Cell    string
	Dialect string
	Line    string
	Verdict Verdict
	Reasons []string
}

// Aggregate is one tier's whole run: the cells the matrix said would run, and
// the results that came back.
type Aggregate struct {
	Tier    int
	Matrix  capabilityprobe.Matrix
	Results []CellResult
}

// Verdicts returns one entry per cell the matrix declared runnable, in matrix
// order, whether or not a result arrived for it.
//
// Iterating the EXPECTED cells rather than the received results is the whole
// point. A report built from the results it happens to have counts the cells
// that ran and calls the rest nothing at all; this repository has been bitten
// by that shape more than once, most recently by a paths filter that removed
// jobs from a pull request where their absence read as approval.
func (a Aggregate) Verdicts() []CellVerdict {
	received := map[string]CellResult{}
	for _, result := range a.Results {
		received[result.Cell] = result
	}
	verdicts := make([]CellVerdict, 0, len(a.Matrix.Cells))
	for _, cell := range a.Matrix.Cells {
		verdicts = append(verdicts, verdictFor(a.Tier, cell, received))
	}
	return verdicts
}

func verdictFor(tier int, cell capabilityprobe.CICell, received map[string]CellResult) CellVerdict {
	result, arrived := received[cell.ID]
	if !arrived {
		return CellVerdict{
			Cell: cell.ID, Dialect: cell.Dialect, Line: cell.Line, Verdict: Missing,
			Reasons: []string{"no result was uploaded for this cell: its job did not run, did not finish, or did not report"},
		}
	}
	if result.Tier != tier {
		return CellVerdict{
			Cell: cell.ID, Dialect: cell.Dialect, Line: cell.Line, Verdict: Missing,
			Reasons: []string{fmt.Sprintf("result reports tier %d; this aggregate requires tier %d", result.Tier, tier)},
		}
	}
	return CellVerdict{
		Cell: cell.ID, Dialect: cell.Dialect, Line: cell.Line,
		Verdict: result.Verdict(), Reasons: result.Reasons(),
	}
}

// Unexpected returns the results that name no runnable cell. They are reported
// rather than ignored: a result for a line the matrix no longer declares means
// the pipeline and the matrix have already drifted apart.
func (a Aggregate) Unexpected() []CellResult {
	var out []CellResult
	for _, result := range a.Results {
		if _, expected := a.Matrix.Find(result.Cell); !expected {
			out = append(out, result)
		}
	}
	return out
}

// Count returns how many cells hold a verdict.
func (a Aggregate) Count(verdict Verdict) int {
	n := 0
	for _, cell := range a.Verdicts() {
		if cell.Verdict == verdict {
			n++
		}
	}
	return n
}

// Err reports why this tier must fail, or nil.
func (a Aggregate) Err() error {
	problems := []error{a.Matrix.Validate()}
	for _, cell := range a.Verdicts() {
		problems = append(problems, verdictProblem(cell))
	}
	for _, result := range a.Unexpected() {
		problems = append(problems, fmt.Errorf(
			"a result arrived for cell %q, which the matrix does not declare runnable", result.Cell))
	}
	return errors.Join(problems...)
}

func verdictProblem(cell CellVerdict) error {
	if cell.Verdict == Passed {
		return nil
	}
	return fmt.Errorf("%s [%s]: %s", cell.Cell, cell.Verdict, joinReasons(cell.Reasons))
}

func joinReasons(reasons []string) string {
	if len(reasons) == 0 {
		return "no reason recorded"
	}
	return strings.Join(reasons, "; ")
}

// WriteAggregate prints the tier report: the census first, then one line per
// cell, then the reasons, then the declared lines this tier could not run.
func WriteAggregate(w io.Writer, a Aggregate) {
	verdicts := a.Verdicts()
	fmt.Fprintf(w, "## Tier %d capability matrix\n\n", a.Tier)
	fmt.Fprintf(w, "Declared release lines: %d. Runnable cells: %d. Results received: %d.\n\n",
		a.Matrix.Declared, len(a.Matrix.Cells), len(a.Results))
	fmt.Fprintf(w, "| Cell | Line | Verdict |\n| --- | --- | --- |\n")
	for _, cell := range verdicts {
		fmt.Fprintf(w, "| `%s` | %s %s | %s |\n", cell.Cell, cell.Dialect, cell.Line, cell.Verdict)
	}
	fmt.Fprintf(w, "\n%d passed, %d capability disagreements, %d suite failures, %d missing.\n",
		a.Count(Passed), a.Count(CapabilityDisagreement), a.Count(SuiteFailure), a.Count(Missing))
	writeFailures(w, a, verdicts)
	writeSkipped(w, a)
}

func writeFailures(w io.Writer, a Aggregate, verdicts []CellVerdict) {
	failed := slices.DeleteFunc(slices.Clone(verdicts), func(cell CellVerdict) bool { return cell.Verdict == Passed })
	if len(failed) == 0 {
		return
	}
	fmt.Fprintf(w, "\n### Why each cell failed\n\n")
	for _, cell := range failed {
		fmt.Fprintf(w, "- **%s** (%s %s) — %s\n", cell.Cell, cell.Dialect, cell.Line, cell.Verdict)
		for _, reason := range cell.Reasons {
			fmt.Fprintf(w, "  - %s\n", reason)
		}
		writeDeferral(w, a, cell)
	}
}

// writeDeferral is the attributability requirement of stokaro/ptah#1341: a
// nightly failure that is a capability disagreement must send the reader to
// the row that already says so rather than to eighteen suite logs.
func writeDeferral(w io.Writer, a Aggregate, cell CellVerdict) {
	if a.Tier < 3 || cell.Verdict != CapabilityDisagreement {
		return
	}
	fmt.Fprintf(w, "  - this is a capability disagreement, not a suite defect: the tier 2 job `%s` "+
		"on every pull request reports the same rows\n", cell.Cell)
}

func writeSkipped(w io.Writer, a Aggregate) {
	if len(a.Matrix.Skipped) == 0 {
		return
	}
	fmt.Fprintf(w, "\n### Declared lines this tier cannot run\n\n")
	for _, cell := range a.Matrix.Skipped {
		fmt.Fprintf(w, "- `%s` (%s %s) — %s\n", cell.ID, cell.Dialect, cell.Line, cell.Skip)
	}
}
