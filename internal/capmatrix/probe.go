package capmatrix

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.5x5.cz/ptah/internal/capabilityprobe"
	"go.5x5.cz/ptah/internal/integrationharness"
)

// RunProbe measures one matrix cell and returns its result.
//
// It never returns an error. Every way a cell can fail — an unreachable
// server, a probe that decided nothing, a preset the server contradicts — is a
// verdict about the cell, and a verdict has to reach the reporting job as a
// result file rather than as a process that died before writing one.
func RunProbe(ctx context.Context, out io.Writer, cell capabilityprobe.CICell, tier int, wait time.Duration) CellResult {
	result := CellResult{
		Cell: cell.ID, Tier: tier, Dialect: cell.Dialect, Line: cell.Line, Image: cell.Image,
		SuiteSkip: cell.SuiteSkip,
	}
	if err := capabilityprobe.WaitForServer(ctx, cell.URL, wait); err != nil {
		result.Probe = ProbeOutcome{Error: err.Error()}
		return result
	}
	report, err := capabilityprobe.Run(ctx, cell.URL)
	if err != nil {
		result.Probe = ProbeOutcome{Error: err.Error()}
		return result
	}
	capabilityprobe.WriteReport(out, report)
	capabilityprobe.WriteEvidence(out, report)
	result.Probe = outcomeFor(cell, report)
	return result
}

func outcomeFor(cell capabilityprobe.CICell, report *capabilityprobe.Report) ProbeOutcome {
	outcome := ProbeOutcome{
		Banner:      report.Banner,
		Version:     report.Version.String(),
		MatchedCell: capabilityprobe.CellID(report.Cell),
		Rows:        len(report.Rows),
		Agrees:      report.Count(capabilityprobe.Agrees),
		Disagrees:   report.Count(capabilityprobe.Disagrees),
		Undecidable: report.Count(capabilityprobe.Undecidable),
		Decided:     report.Decided(),
		Floor:       report.Floor(),
		Mismatches:  mismatches(report),
	}
	outcome.Error = errorText(cell, report, outcome)
	outcome.OK = outcome.Error == ""
	return outcome
}

func mismatches(report *capabilityprobe.Report) []string {
	var out []string
	for _, row := range report.Mismatches() {
		out = append(out, fmt.Sprintf("%s: preset says %t, server does %t [%s]",
			row.Capability, row.PresetSays, row.ServerDoes, row.Outcome))
	}
	return out
}

// errorText combines the probe's own verdict with the one question only the
// pipeline can ask: whether the server that answered is on the line this job
// was fanned out for.
//
// The probe cannot ask it. Handed a URL, it measures whichever server is
// there and reports the cell that server falls on, which is the right behavior
// for a local run. In the pipeline the cell is chosen first and the container
// started from it, so a tag that resolves to a different release line — a
// floating alias, a registry that moved a tag, a copy-paste in a matrix entry —
// would otherwise be recorded as a clean measurement of the wrong line.
func errorText(cell capabilityprobe.CICell, report *capabilityprobe.Report, outcome ProbeOutcome) string {
	var text string
	if outcome.MatchedCell != cell.ID {
		text = fmt.Sprintf("this job started %s for cell %s, and the server that answered reports %s, "+
			"which falls on cell %s", cell.Image, cell.ID, report.Version, outcome.MatchedCell)
	}
	err := report.Err()
	if err == nil {
		return text
	}
	if text == "" {
		return err.Error()
	}
	return text + "\n" + err.Error()
}

// RecordSuite folds the integration runner's result into a cell that has
// already been probed.
func RecordSuite(result CellResult, exitCode int, reportDir string) (CellResult, error) {
	suite := &SuiteOutcome{ExitCode: exitCode}
	report, err := readSuiteReport(reportDir)
	if err != nil {
		suite.Error = err.Error()
		result.Suite = suite
		return result, nil
	}
	suite.Total = report.TotalTests
	suite.Passed = report.PassedTests
	suite.Failed = report.FailedTests
	suite.Skipped = report.SkippedTests
	suite.Error = suiteProblem(suite)
	suite.OK = suite.Error == ""
	result.Suite = suite
	return result, nil
}

// suiteProblem says why a suite half failed.
//
// The executed-nothing case is the one a bare exit code cannot see: the
// integration runner exits 0 when every requested scenario skipped, so a cell
// whose URL never reached the runner would report a green suite having run no
// test at all.
func suiteProblem(suite *SuiteOutcome) string {
	executed := suite.Total - suite.Skipped
	switch {
	case suite.ExitCode != 0:
		return fmt.Sprintf("the integration suite exited %d with %d failures out of %d tests",
			suite.ExitCode, suite.Failed, suite.Total)
	case suite.Failed > 0:
		return fmt.Sprintf("the integration suite exited 0 and reported %d failures out of %d tests",
			suite.Failed, suite.Total)
	case executed <= 0:
		return fmt.Sprintf("the integration suite executed no test at all (%d skipped of %d); "+
			"a suite that ran nothing must not read as a suite that passed", suite.Skipped, suite.Total)
	default:
		return ""
	}
}

// readSuiteReport finds the runner's JSON report. The runner names it after
// the wall clock, so the directory is searched rather than one path assumed,
// and two reports in one directory is an error: a leftover from an earlier run
// would otherwise be read as this run's answer.
func readSuiteReport(dir string) (integrationharness.TestReport, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*-report.json"))
	if err != nil {
		return integrationharness.TestReport{}, fmt.Errorf("search %s for the suite report: %w", dir, err)
	}
	if len(matches) != 1 {
		return integrationharness.TestReport{}, fmt.Errorf(
			"expected exactly one *-report.json under %s and found %d, so the suite's own counts cannot be read",
			dir, len(matches))
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		return integrationharness.TestReport{}, fmt.Errorf("read %s: %w", matches[0], err)
	}
	var report integrationharness.TestReport
	if err := json.Unmarshal(body, &report); err != nil {
		return integrationharness.TestReport{}, fmt.Errorf("decode %s: %w", matches[0], err)
	}
	return report, nil
}
