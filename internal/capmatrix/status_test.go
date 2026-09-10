package capmatrix_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/capmatrix"
)

// matrix builds a two-engine matrix with one line the tier cannot run.
//
// The skipped line is the shape both the file and the badge get wrong when they
// are written from the results rather than from the declaration: it produces no
// result, and a reader built around received results drops it silently.
func matrix() capabilityprobe.Matrix {
	return capabilityprobe.Matrix{
		Declared: 4,
		Cells: []capabilityprobe.CICell{
			{ID: "postgres-18", Dialect: "postgres", Line: "18", Support: capability.Certified},
			{ID: "postgres-17", Dialect: "postgres", Line: "17", Support: capability.Certified},
			{ID: "mysql-8-4", Dialect: "mysql", Line: "8.4", Support: capability.Certified},
		},
		Skipped: []capabilityprobe.CICell{
			{ID: "sqlite-3", Dialect: "sqlite", Line: "3", Skip: "no container image is declared for this line"},
		},
	}
}

// passed is the result a cell uploads when the tier asked it everything and the
// server agreed. On tier 2 an OK probe is the whole answer; the suite belongs to
// tier 3.
func passed(cell, dialect, line string) capmatrix.CellResult {
	return capmatrix.CellResult{
		Cell: cell, Tier: 2, Dialect: dialect, Line: line,
		Probe: capmatrix.ProbeOutcome{OK: true},
	}
}

func provenance() capmatrix.Provenance {
	return capmatrix.Provenance{
		Measured: "2026-09-09T18:44:12Z",
		Commit:   "cd04d0bed4c1b0a1",
		RunID:    "34390826860",
		RunURL:   "https://github.com/stokaro/ptah/actions/runs/34390826860",
	}
}

// TestWriteStatus_NamesEveryDeclaredLine is the rule the file exists for.
//
// A status file assembled from the results that arrived describes a smaller
// matrix every time a cell fails to start, and an absent row reads exactly like
// a passing one. Both cells here are missing, and both still have to appear.
func TestWriteStatus_NamesEveryDeclaredLine(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	capmatrix.WriteStatus(&out, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())
	rendered := out.String()

	c.Assert(rendered, qt.Contains, capmatrix.StatusHeader)
	c.Assert(rendered, qt.Contains, "| `postgres-18` | postgres | 18 | MISSING |")
	c.Assert(rendered, qt.Contains, "| `postgres-17` | postgres | 17 | MISSING |")
	c.Assert(rendered, qt.Contains, "| `mysql-8-4` | mysql | 8.4 | MISSING |")
	c.Assert(rendered, qt.Contains, "- Declared release lines: 4. Runnable cells: 3. Results received: 0.")
	c.Assert(rendered, qt.Contains, "- 0 passed, 0 capability disagreements, 0 suite failures, 3 missing.")
}

// TestWriteStatus_CarriesTheRunThatProducedIt pins the provenance line.
//
// A verdict a reader cannot trace to the run behind it is an assertion rather
// than evidence, and the file outlives the run summary that would otherwise
// answer where it came from.
func TestWriteStatus_CarriesTheRunThatProducedIt(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	capmatrix.WriteStatus(&out, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())
	rendered := out.String()

	c.Assert(rendered, qt.Contains, "- Measured: 2026-09-09T18:44:12Z")
	c.Assert(rendered, qt.Contains, "- Commit: `cd04d0bed4c1b0a1`")
	c.Assert(rendered, qt.Contains,
		"- Run: [34390826860](https://github.com/stokaro/ptah/actions/runs/34390826860)")
}

// TestWriteStatus_NamesTheLinesTheTierCannotRun keeps the census whole.
//
// The identity is declared == runnable + skipped. A file carrying only the
// runnable half reports a smaller matrix than the repository declares, and the
// reason a line is unrunnable is the part a reader needs.
func TestWriteStatus_NamesTheLinesTheTierCannotRun(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	capmatrix.WriteStatus(&out, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())

	c.Assert(out.String(), qt.Contains, "## Declared lines this tier cannot run")
	c.Assert(out.String(), qt.Contains,
		"- `sqlite-3` (sqlite 3) — no container image is declared for this line")
}

// TestWriteStatus_IsDeterministic is what lets a run that measured no change
// leave the open pull request alone instead of pushing an identical commit.
//
// The writer reads no clock and no environment: every varying value arrives in
// Provenance, so the same aggregate renders the same bytes.
func TestWriteStatus_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	var first, second bytes.Buffer
	capmatrix.WriteStatus(&first, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())
	capmatrix.WriteStatus(&second, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())

	c.Assert(second.String(), qt.Equals, first.String())
}

// TestVerdictOf_IgnoresTheStampAndKeepsTheMeasurement is what stops the status
// pull request reopening every night.
//
// The three provenance lines move on every run. A byte comparison would report
// a change each time, publish a commit nobody disagreed with, and lose the one
// question the file is good at answering: when did this verdict last change.
func TestVerdictOf_IgnoresTheStampAndKeepsTheMeasurement(t *testing.T) {
	c := qt.New(t)

	var first, second bytes.Buffer
	capmatrix.WriteStatus(&first, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())
	capmatrix.WriteStatus(&second, capmatrix.Aggregate{Tier: 2, Matrix: matrix()},
		capmatrix.Provenance{
			Measured: "2026-12-31T23:59:59Z",
			Commit:   "0000000000000000",
			RunID:    "999",
			RunURL:   "https://github.com/stokaro/ptah/actions/runs/999",
		})

	c.Assert(second.String(), qt.Not(qt.Equals), first.String())
	c.Assert(capmatrix.VerdictOf(second.String()), qt.Equals, capmatrix.VerdictOf(first.String()))
	c.Assert(capmatrix.VerdictOf(first.String()), qt.Contains, "| `postgres-18` | postgres | 18 | MISSING |")
}

// TestVerdictOf_SeesAChangedVerdict is the control for the row above.
//
// A comparison that ignored too much would call every run unchanged and publish
// nothing ever, which reads exactly like a matrix that never moves.
func TestVerdictOf_SeesAChangedVerdict(t *testing.T) {
	c := qt.New(t)

	var missing, passing bytes.Buffer
	capmatrix.WriteStatus(&missing, capmatrix.Aggregate{Tier: 2, Matrix: matrix()}, provenance())
	capmatrix.WriteStatus(&passing, capmatrix.Aggregate{
		Tier: 2, Matrix: matrix(),
		Results: []capmatrix.CellResult{passed("postgres-18", "postgres", "18")},
	}, provenance())

	c.Assert(capmatrix.VerdictOf(passing.String()), qt.Not(qt.Equals), capmatrix.VerdictOf(missing.String()))
}
