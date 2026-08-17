package capmatrix

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// CellResult is one matrix cell's verdict, written by the job that ran it and
// read back by the job that reports the tier.
//
// It travels as a file rather than as a job output because a matrix job cannot
// hand a per-cell output to a dependent job: every cell would write to the same
// key and the last one to finish would be the only one anybody read.
type CellResult struct {
	// Cell is the CICell id the result belongs to.
	Cell string `json:"cell"`
	// Tier is 2 for the per-pull-request capability fan-out and 3 for the
	// nightly suite fan-out.
	Tier int `json:"tier"`

	Dialect string `json:"dialect"`
	Line    string `json:"line"`
	Image   string `json:"image,omitempty"`

	// Probe is what the capability probe found. Every tier runs it.
	Probe ProbeOutcome `json:"probe"`

	// Suite is what the integration runner found, present on tier 3 only.
	Suite *SuiteOutcome `json:"suite,omitempty"`

	// SuiteSkip carries the cell's declared reason for having no
	// integration-runner target, so a tier that runs no suite for it is read as
	// a decision rather than as a result somebody lost.
	SuiteSkip string `json:"suite_skip,omitempty"`
}

// ProbeOutcome is the capability probe's half of a cell.
type ProbeOutcome struct {
	OK bool `json:"ok"`
	// Banner and Version are what the server that actually answered said it
	// was, so a cell that started the wrong image is visible in the result
	// rather than only in the log.
	Banner  string `json:"banner,omitempty"`
	Version string `json:"version,omitempty"`
	// MatchedCell is the matrix cell the live server fell on. A value other
	// than Cell means the container resolved to a different release line than
	// the one this job was fanned out for.
	MatchedCell string `json:"matched_cell,omitempty"`

	Rows        int `json:"rows"`
	Agrees      int `json:"agrees"`
	Disagrees   int `json:"disagrees"`
	Undecidable int `json:"undecidable"`
	Decided     int `json:"decided"`
	Floor       int `json:"floor"`

	// Mismatches names every capability whose observation contradicts the
	// preset, in the probe's own words.
	Mismatches []string `json:"mismatches,omitempty"`
	// Error is why the probe failed, empty when OK.
	Error string `json:"error,omitempty"`
}

// SuiteOutcome is the integration runner's half of a tier 3 cell.
type SuiteOutcome struct {
	OK       bool `json:"ok"`
	ExitCode int  `json:"exit_code"`
	Total    int  `json:"total"`
	Passed   int  `json:"passed"`
	Failed   int  `json:"failed"`
	Skipped  int  `json:"skipped"`
	// Error is why the suite half failed, empty when OK.
	Error string `json:"error,omitempty"`
}

// Verdict classifies a cell for the tier report.
type Verdict string

const (
	// Passed: everything this tier asked of the cell was answered and agreed.
	Passed Verdict = "PASS"
	// CapabilityDisagreement: the live server contradicts the preset, or the
	// probe could not measure the line at all. On tier 3 this is the verdict
	// that defers: the tier 2 job for the same cell reports the same rows, and
	// a suite failure underneath it is downstream of a model that is already
	// known to be wrong.
	CapabilityDisagreement Verdict = "CAPABILITY"
	// SuiteFailure: the capability model agrees and the integration suite
	// still failed, so the failure belongs to the code and not to the preset.
	SuiteFailure Verdict = "SUITE"
	// Missing: the cell produced no result at all. A cancelled, skipped or
	// never-started job lands here, because an absent check reads exactly like
	// a passing one.
	Missing Verdict = "MISSING"
)

// Verdict returns the cell's classification.
func (r CellResult) Verdict() Verdict {
	switch {
	case !r.Probe.OK:
		return CapabilityDisagreement
	// Missing means a suite result that should have arrived did not. A cell
	// that declares no runner target was never going to produce one, and
	// treating its absence as a loss would fail the night for a dialect doing
	// exactly what it said it would do.
	case r.Tier == 3 && r.Suite == nil && r.SuiteSkip == "":
		return Missing
	case r.Suite != nil && !r.Suite.OK:
		return SuiteFailure
	default:
		return Passed
	}
}

// Reasons explains the verdict in one line per cause.
func (r CellResult) Reasons() []string {
	var reasons []string
	reasons = append(reasons, r.probeReasons()...)
	if r.Tier == 3 && r.Suite == nil {
		reasons = append(reasons, "the integration suite produced no recorded outcome")
	}
	if r.Suite != nil && !r.Suite.OK {
		reasons = append(reasons, r.suiteReason())
	}
	return reasons
}

func (r CellResult) probeReasons() []string {
	if r.Probe.OK {
		return nil
	}
	reasons := slices.Clone(r.Probe.Mismatches)
	if r.Probe.Error != "" {
		reasons = append(reasons, strings.Split(r.Probe.Error, "\n")...)
	}
	if len(reasons) == 0 {
		return []string{"the capability probe failed and said nothing about why"}
	}
	return reasons
}

func (r CellResult) suiteReason() string {
	if r.Suite.Error != "" {
		return r.Suite.Error
	}
	return fmt.Sprintf("the integration suite exited %d: %d of %d tests failed, %d skipped",
		r.Suite.ExitCode, r.Suite.Failed, r.Suite.Total, r.Suite.Skipped)
}

// WriteResult writes a cell result where the reporting job will find it.
func WriteResult(path string, result CellResult) error {
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode the result for cell %s: %w", result.Cell, err)
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create the result directory %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		return fmt.Errorf("write the result for cell %s: %w", result.Cell, err)
	}
	return nil
}

// ReadResults reads every cell result under a directory, recursively.
//
// The walk is recursive because the reporting job downloads one artifact per
// cell and they arrive in per-artifact subdirectories unless the download is
// asked to merge them. A reader that only looked at the top level would find
// nothing and, without the census in Aggregate, would report a tier that ran
// no cells as a tier with no failures.
func ReadResults(dir string) ([]CellResult, error) {
	var results []CellResult
	var problems []error
	seen := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		result, readErr := readResult(path)
		if readErr != nil {
			problems = append(problems, readErr)
			return nil
		}
		if first, duplicate := seen[result.Cell]; duplicate {
			problems = append(problems, fmt.Errorf(
				"cell %s has two results, %s and %s; one of them is stale and neither can be trusted",
				result.Cell, first, path))
			return nil
		}
		seen[result.Cell] = path
		results = append(results, result)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read the results under %s: %w", dir, err)
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Cell < results[j].Cell })
	return results, nil
}

// ReadResult reads one cell result written by WriteResult.
func ReadResult(path string) (CellResult, error) {
	return readResult(path)
}

func readResult(path string) (CellResult, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return CellResult{}, fmt.Errorf("read %s: %w", path, err)
	}
	var result CellResult
	if err := json.Unmarshal(body, &result); err != nil {
		return CellResult{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if result.Cell == "" {
		return CellResult{}, fmt.Errorf("%s names no cell, so its verdict belongs to nothing", path)
	}
	return result, nil
}
