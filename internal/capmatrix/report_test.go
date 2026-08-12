package capmatrix_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capabilityprobe"
	"go.5x5.cz/ptah/internal/capmatrix"
)

// twoCellMatrix is a matrix small enough to reason about: two runnable lines
// and one declared line no tier can run.
func twoCellMatrix() capabilityprobe.Matrix {
	return capabilityprobe.Matrix{
		Declared: 3,
		Cells: []capabilityprobe.CICell{
			{ID: "postgres-17", Dialect: "postgres", Line: "17", Runnable: true, URL: "postgres://x", DockerRun: []string{"postgres:17"}},
			{ID: "mariadb-11-4", Dialect: "mariadb", Line: "11.4", Runnable: true, URL: "mariadb://x", DockerRun: []string{"mariadb:11.4"}},
		},
		Skipped: []capabilityprobe.CICell{
			{ID: "spanner-0", Dialect: "spanner", Line: "0", Skip: "no container image is declared for this line"},
		},
	}
}

func passing(cell string) capmatrix.CellResult {
	return capmatrix.CellResult{
		Cell: cell, Tier: 2, Probe: capmatrix.ProbeOutcome{OK: true, Rows: 25, Decided: 25, Floor: 25},
	}
}

// TestAggregate_HappyPath is the only shape that may report success: every
// runnable cell came back, and came back agreeing.
func TestAggregate_HappyPath(t *testing.T) {
	c := qt.New(t)

	aggregate := capmatrix.Aggregate{
		Tier:    2,
		Matrix:  twoCellMatrix(),
		Results: []capmatrix.CellResult{passing("postgres-17"), passing("mariadb-11-4")},
	}

	c.Assert(aggregate.Err(), qt.IsNil)
	c.Assert(aggregate.Count(capmatrix.Passed), qt.Equals, 2)
	c.Assert(aggregate.Count(capmatrix.Missing), qt.Equals, 0)

	var out strings.Builder
	capmatrix.WriteAggregate(&out, aggregate)
	c.Assert(out.String(), qt.Contains, "Declared release lines: 3. Runnable cells: 2. Results received: 2.")
	c.Assert(out.String(), qt.Contains,
		"- `spanner-0` (spanner 0) — no container image is declared for this line",
		qt.Commentf("a line no tier can run has to stay visible in the report, or the pipeline "+
			"silently shrinks to whatever it can already do"))
}

// TestAggregate_FailurePath covers every way a tier must fail.
//
// The missing-cell row is the one this whole aggregation exists for. A report
// assembled from the results it received would count two cells, find no
// failure among them and pass, while the third cell's job never ran at all —
// the exact shape a paths filter, a cancelled matrix leg, or a fan-out that
// produced zero jobs takes in this repository's CI.
func TestAggregate_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name    string
		tier    int
		results []capmatrix.CellResult
		assert  func(c *qt.C, aggregate capmatrix.Aggregate)
	}{{
		name:    "a cell that produced no result fails the tier",
		tier:    2,
		results: []capmatrix.CellResult{passing("postgres-17")},
		assert: func(c *qt.C, aggregate capmatrix.Aggregate) {
			c.Assert(aggregate.Count(capmatrix.Missing), qt.Equals, 1)
			c.Assert(aggregate.Err(), qt.ErrorMatches, "(?s).*mariadb-11-4 \\[MISSING\\]: no result was uploaded.*")
		},
	}, {
		name:    "no results at all fails the tier",
		tier:    2,
		results: nil,
		assert: func(c *qt.C, aggregate capmatrix.Aggregate) {
			c.Assert(aggregate.Count(capmatrix.Missing), qt.Equals, 2)
			c.Assert(aggregate.Err(), qt.ErrorMatches, "(?s).*postgres-17 \\[MISSING\\].*mariadb-11-4 \\[MISSING\\].*")
		},
	}, {
		name: "a capability disagreement fails the tier and names the row",
		tier: 2,
		results: []capmatrix.CellResult{passing("postgres-17"), {
			Cell: "mariadb-11-4", Tier: 2,
			Probe: capmatrix.ProbeOutcome{Mismatches: []string{"advisory_locks: preset says true, server does false [DISAGREES]"}},
		}},
		assert: func(c *qt.C, aggregate capmatrix.Aggregate) {
			c.Assert(aggregate.Count(capmatrix.CapabilityDisagreement), qt.Equals, 1)
			c.Assert(aggregate.Err(), qt.ErrorMatches, "(?s).*mariadb-11-4 \\[CAPABILITY\\]: advisory_locks: preset says true.*")
		},
	}, {
		name: "a suite failure under an agreeing preset is attributed to the suite",
		tier: 3,
		results: []capmatrix.CellResult{passing("postgres-17"), {
			Cell: "mariadb-11-4", Tier: 3,
			Probe: capmatrix.ProbeOutcome{OK: true, Decided: 25, Floor: 25},
			Suite: &capmatrix.SuiteOutcome{ExitCode: 1, Total: 40, Failed: 2, Error: "the integration suite exited 1 with 2 failures out of 40 tests"},
		}},
		assert: func(c *qt.C, aggregate capmatrix.Aggregate) {
			c.Assert(aggregate.Count(capmatrix.SuiteFailure), qt.Equals, 1)
			c.Assert(aggregate.Err(), qt.ErrorMatches, "(?s).*mariadb-11-4 \\[SUITE\\]: the integration suite exited 1.*")
		},
	}, {
		name: "a result for a line the matrix does not declare is reported",
		tier: 2,
		results: []capmatrix.CellResult{
			passing("postgres-17"), passing("mariadb-11-4"), passing("clickhouse-25-8"),
		},
		assert: func(c *qt.C, aggregate capmatrix.Aggregate) {
			c.Assert(aggregate.Err(), qt.ErrorMatches, `(?s).*a result arrived for cell "clickhouse-25-8".*`)
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			tc.assert(c, capmatrix.Aggregate{Tier: tc.tier, Matrix: twoCellMatrix(), Results: tc.results})
		})
	}
}

// TestAggregate_RefusesAMatrixThatWouldRunNothing is the mutant the tiered
// pipeline is most exposed to: a fan-out computed from an empty list produces
// no jobs, and a tier with no jobs has nothing to fail.
func TestAggregate_RefusesAMatrixThatWouldRunNothing(t *testing.T) {
	c := qt.New(t)

	aggregate := capmatrix.Aggregate{Tier: 2, Matrix: capabilityprobe.Matrix{}}

	c.Assert(aggregate.Err(), qt.ErrorMatches, "(?s).*declares no release lines.*")
	c.Assert(aggregate.Verdicts(), qt.HasLen, 0)
}

// TestWriteAggregate_TierThreeDefersToTierTwo covers the attributability
// requirement: a nightly capability failure has to send the reader to the row
// that already says so instead of to eighteen suite logs.
func TestWriteAggregate_TierThreeDefersToTierTwo(t *testing.T) {
	c := qt.New(t)

	disagreeing := capmatrix.CellResult{
		Cell: "mariadb-11-4", Tier: 3,
		Probe: capmatrix.ProbeOutcome{Mismatches: []string{"sequences: preset says false, server does true [DISAGREES]"}},
		Suite: &capmatrix.SuiteOutcome{ExitCode: 1, Total: 40, Failed: 7},
	}

	for _, tc := range []struct {
		name   string
		tier   int
		assert func(c *qt.C, report string)
	}{{
		name: "tier 3 names the tier 2 job",
		tier: 3,
		assert: func(c *qt.C, report string) {
			c.Assert(report, qt.Contains, "the tier 2 job `mariadb-11-4`")
			c.Assert(report, qt.Contains, "sequences: preset says false, server does true")
		},
	}, {
		name: "tier 2 does not defer to itself",
		tier: 2,
		assert: func(c *qt.C, report string) {
			c.Assert(report, qt.Not(qt.Contains), "the tier 2 job `mariadb-11-4`")
			c.Assert(report, qt.Contains, "sequences: preset says false, server does true")
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			var out strings.Builder
			capmatrix.WriteAggregate(&out, capmatrix.Aggregate{
				Tier:    tc.tier,
				Matrix:  twoCellMatrix(),
				Results: []capmatrix.CellResult{passing("postgres-17"), disagreeing},
			})
			tc.assert(c, out.String())
		})
	}
}

// TestCellResult_Verdict pins the classification a tier 3 report attributes
// with. A cell whose probe disagreed is a capability failure even when the
// suite failed underneath it, because the suite ran against a model already
// known to be wrong.
func TestCellResult_Verdict(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name   string
		result capmatrix.CellResult
		want   capmatrix.Verdict
	}{{
		name:   "probe agrees and no suite ran",
		result: capmatrix.CellResult{Probe: capmatrix.ProbeOutcome{OK: true}},
		want:   capmatrix.Passed,
	}, {
		name:   "probe agrees and the suite passed",
		result: capmatrix.CellResult{Probe: capmatrix.ProbeOutcome{OK: true}, Suite: &capmatrix.SuiteOutcome{OK: true}},
		want:   capmatrix.Passed,
	}, {
		name:   "probe disagrees",
		result: capmatrix.CellResult{Probe: capmatrix.ProbeOutcome{}},
		want:   capmatrix.CapabilityDisagreement,
	}, {
		name:   "probe disagrees and the suite failed too",
		result: capmatrix.CellResult{Probe: capmatrix.ProbeOutcome{}, Suite: &capmatrix.SuiteOutcome{}},
		want:   capmatrix.CapabilityDisagreement,
	}, {
		name:   "the suite failed under an agreeing probe",
		result: capmatrix.CellResult{Probe: capmatrix.ProbeOutcome{OK: true}, Suite: &capmatrix.SuiteOutcome{}},
		want:   capmatrix.SuiteFailure,
	}} {
		c.Run(tc.name, func(c *qt.C) {
			c.Assert(tc.result.Verdict(), qt.Equals, tc.want)
		})
	}
}
