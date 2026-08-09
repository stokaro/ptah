package capabilityprobe

// White-box testing required: the properties that keep this harness from
// manufacturing evidence are properties of unexported machinery. The
// statement table (plan), the
// three-way outcome assignment (assemble), and the requirement ordering
// (unmetRequirement) are not reachable from outside the package, and testing
// them through a live server instead would make the guard depend on which
// container happens to be running.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// TestPlans_AnswerEveryRegisteredCapabilityExactlyOnce is the guard that keeps
// the matrix complete as the registry grows.
//
// Without it, adding a capability to core/platform/capability would leave every
// dialect silently missing a row while the report still called the run
// complete. "Exactly once" rather than "at least once" is deliberate: two
// experiments deciding the same key would let the later one overwrite the
// earlier answer depending on table order.
func TestPlans_AnswerEveryRegisteredCapabilityExactlyOnce(t *testing.T) {
	c := qt.New(t)

	registered := map[capability.Capability]bool{}
	for _, key := range capability.All() {
		registered[key] = true
	}

	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB} {
		c.Run(dialect, func(c *qt.C) {
			dialectPlan, ok := planFor(dialect)
			c.Assert(ok, qt.IsTrue)

			answered := map[capability.Capability]int{}
			for key := range dialectPlan.undecided {
				answered[key]++
			}
			for _, current := range dialectPlan.experiments {
				c.Assert(len(current.decides) > 0, qt.IsTrue, qt.Commentf("an experiment that decides nothing cannot be run"))
				for _, key := range current.decides {
					answered[key]++
				}
			}

			for key := range registered {
				c.Assert(answered[key], qt.Equals, 1,
					qt.Commentf("%s: capability %q is answered %d times, want exactly once", dialect, key, answered[key]))
			}
			for key := range answered {
				c.Assert(registered[key], qt.IsTrue,
					qt.Commentf("%s: plan answers %q, which is not in the capability registry", dialect, key))
			}
		})
	}
}

// TestPlans_DeclaredUndecidablesCarryAReason pins the shape that makes
// undecidable an answer rather than a shrug.
func TestPlans_DeclaredUndecidablesCarryAReason(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB} {
		c.Run(dialect, func(c *qt.C) {
			dialectPlan, ok := planFor(dialect)
			c.Assert(ok, qt.IsTrue)
			for key, reason := range dialectPlan.undecided {
				c.Assert(len(reason) > 40, qt.IsTrue,
					qt.Commentf("%s/%s: an undecidable reason has to say why, not just that", dialect, key))
			}
		})
	}
}

// reportOn builds a minimal report for the assembly tests.
func reportOn(cell Cell, matched bool, preset capability.Capabilities) *Report {
	return &Report{
		Dialect:             cell.Dialect,
		Version:             Version{Numbers: []int{17, 10}},
		Cell:                cell,
		Matched:             matched,
		SessionCapabilities: preset,
	}
}

var measuredCell = Cell{
	Dialect: platform.Postgres, Line: "17",
	Preset: capability.Postgres17, PresetName: "Postgres17",
	Refinement: RefinedByVersion,
}

// TestAssemble_ThreeOutcomes pins the whole verdict table, including the one
// row a cheaper implementation folds into agreement.
func TestAssemble_ThreeOutcomes(t *testing.T) {
	c := qt.New(t)

	const key = capability.XMLType // Postgres17 says true.

	for _, tc := range []struct {
		name   string
		obs    observation
		assert func(c *qt.C, row Row)
	}{{
		name: "the server does what the preset says",
		obs:  decided(true),
		assert: func(c *qt.C, row Row) {
			c.Assert(row.Outcome, qt.Equals, Agrees)
			c.Assert(row.Observed, qt.IsTrue)
			c.Assert(row.Reason, qt.Equals, "")
		},
	}, {
		name: "the server does not, which is a disagreement and not a warning",
		obs:  decided(false),
		assert: func(c *qt.C, row Row) {
			c.Assert(row.Outcome, qt.Equals, Disagrees)
			c.Assert(row.Mismatch(), qt.IsTrue)
		},
	}, {
		name: "an undecided key stays undecidable and never becomes agreement",
		obs:  cannotDecide("the precondition was refused"),
		assert: func(c *qt.C, row Row) {
			c.Assert(row.Outcome, qt.Equals, Undecidable)
			c.Assert(row.Observed, qt.IsFalse,
				qt.Commentf("an undecided row must not carry a server answer it never obtained"))
			c.Assert(row.Reason, qt.Equals, "the precondition was refused")
			c.Assert(row.Mismatch(), qt.IsFalse)
		},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			report := reportOn(measuredCell, true, capability.Postgres17())
			rows := assemble(report, map[capability.Capability]observation{key: tc.obs}, nil)
			tc.assert(c, rowFor(c, rows, key))
		})
	}
}

// TestAssemble_UndecidableRowsAreNotCountedAsDecided is the counter to the
// cheapest wrong implementation of this harness: reporting every row it could
// not decide as agreeing, so the matrix comes out green.
func TestAssemble_UndecidableRowsAreNotCountedAsDecided(t *testing.T) {
	c := qt.New(t)

	report := reportOn(measuredCell, true, capability.Postgres17())
	observations := map[capability.Capability]observation{}
	for _, key := range capability.All() {
		observations[key] = cannotDecide("nothing was executed for this key")
	}
	report.Rows = assemble(report, observations, nil)
	report.Planned = true
	report.Control = Attempt{Statement: nonsenseControl}

	c.Assert(report.Count(Undecidable), qt.Equals, len(capability.All()))
	c.Assert(report.Count(Agrees), qt.Equals, 0)
	c.Assert(report.Decided(), qt.Equals, 0)
	c.Assert(report.Err(), qt.ErrorMatches, `(?s).*decided 0 of \d+ capability rows.*`)
}

// TestAssemble_AnUnattributableLineKeepsTheObservation covers the six dialects
// whose preset is not selected by a version.
//
// The rows must be undecidable — an observation on one CockroachDB release is
// being credited to every other release, which is not a measurement of this
// line — and the observation must survive anyway, so a contradiction found
// there is reported rather than absorbed by the word UNDECIDABLE.
func TestAssemble_AnUnattributableLineKeepsTheObservation(t *testing.T) {
	c := qt.New(t)

	bannerCell := Cell{
		Dialect: platform.CockroachDB, Line: "26.2",
		Preset: capability.CockroachDB23, PresetName: "CockroachDB23",
		Refinement: RefinedByBanner,
	}
	report := reportOn(bannerCell, true, capability.CockroachDB23())
	report.Planned = true
	report.Control = Attempt{Statement: nonsenseControl}
	// What a live CockroachDB actually resolves to. VersionSpecific is TRUE
	// here even though no version was consulted, which is why undecidability
	// has to come from the declared refinement: an implementation that read
	// this field would credit the observation to a line the resolver never
	// distinguished from its siblings.
	report.Resolution.VersionSpecific = true
	report.Resolution.Capabilities = capability.CockroachDB23()
	report.Rows = assemble(report, map[capability.Capability]observation{
		capability.Sequences: decided(true), // CockroachDB23 says false.
		capability.Views:     decided(true), // CockroachDB23 says true.
	}, nil)

	sequences := rowFor(c, report.Rows, capability.Sequences)
	c.Assert(sequences.Outcome, qt.Equals, Undecidable)
	c.Assert(sequences.Observed, qt.IsTrue)
	c.Assert(sequences.ServerDoes, qt.IsTrue)
	c.Assert(sequences.Reason, qt.Contains, "banner substring")

	views := rowFor(c, report.Rows, capability.Views)
	c.Assert(views.Outcome, qt.Equals, Undecidable)

	c.Assert(report.Mismatches(), qt.HasLen, 1)
	c.Assert(report.Mismatches()[0].Capability, qt.Equals, capability.Sequences)
	c.Assert(report.Err(), qt.ErrorMatches, `(?s).*sequences: preset says false, server does true.*`)
}

// TestReportErr covers the ways a run must refuse to report success.
func TestReportErr(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name  string
		build func() *Report
		want  string
	}{{
		name: "a matched, measured, fully decided run passes",
		build: func() *Report {
			return decidedReport(measuredCell, true)
		},
		want: "",
	}, {
		name: "a server on no declared line fails",
		build: func() *Report {
			report := decidedReport(measuredCell, true)
			report.Matched = false
			report.Cell = Cell{}
			return report
		},
		want: `(?s).*this release line is not in the matrix.*`,
	}, {
		name: "a line with no measured preset fails",
		build: func() *Report {
			report := decidedReport(Cell{
				Dialect: platform.Postgres, Line: "18",
				Refinement: RefinedByVersion,
				Note:       "no measured PostgreSQL 18 preset",
			}, true)
			return report
		},
		want: `(?s).*has no measured capability preset.*`,
	}, {
		name: "a saturated resolution contradicts a measured cell",
		build: func() *Report {
			report := decidedReport(measuredCell, true)
			report.Resolution.Saturated = true
			report.Resolution.NewestMeasured = "17.x"
			return report
		},
		want: `(?s).*past the newest measured line.*`,
	}, {
		name: "a cell naming a preset the resolver did not hand out fails",
		build: func() *Report {
			report := decidedReport(measuredCell, true)
			report.Resolution.Capabilities = capability.Postgres13()
			return report
		},
		want: `(?s).*names preset Postgres17, but the resolver handed this server a different set.*`,
	}, {
		name: "a server that accepts the nonsense control invalidates the run",
		build: func() *Report {
			report := decidedReport(measuredCell, true)
			report.Control = Attempt{Statement: nonsenseControl, Accepted: true}
			return report
		},
		want: `(?s).*ACCEPTED the nonsense control.*`,
	}, {
		name: "a dialect with no statement table fails rather than reporting agreement",
		build: func() *Report {
			report := decidedReport(measuredCell, true)
			report.Planned = false
			return report
		},
		want: `(?s).*no statement table for the postgres dialect.*`,
	}} {
		c.Run(tc.name, func(c *qt.C) {
			assertErrMatches(c, tc.build().Err(), tc.want)
		})
	}
}

// TestRun_RefusesAnEmptyMatrix pins the guard that stops a matrix covering
// nothing from reporting a clean run. The URL is never dialed: the refusal
// happens before any connection is attempted, which is what makes it a matrix
// guard rather than a connection error.
func TestRun_RefusesAnEmptyMatrix(t *testing.T) {
	c := qt.New(t)

	original := Cells
	defer func() { Cells = original }()
	Cells = nil

	_, err := Run(context.Background(), "postgres://nobody@127.0.0.1:1/none")
	c.Assert(err, qt.ErrorMatches, `the capability matrix declares no cells; refusing to report a vacuous pass`)
}

// TestUnmetRequirement pins the ordering the registry's own edge implies: on a
// server without the generic DROP CONSTRAINT clause, the guarded spelling is
// refused for the missing clause, so scoring the guard false would answer a
// question the run never asked.
func TestUnmetRequirement(t *testing.T) {
	c := qt.New(t)

	guardExperiment := experiment{
		decides:  []capability.Capability{capability.DropConstraintIfExists},
		requires: []capability.Capability{capability.DropConstraintGeneric},
	}
	for _, tc := range []struct {
		name         string
		observations map[capability.Capability]observation
		wantMet      bool
	}{{
		name:         "requirement decided true",
		observations: map[capability.Capability]observation{capability.DropConstraintGeneric: decided(true)},
		wantMet:      true,
	}, {
		name:         "requirement decided false",
		observations: map[capability.Capability]observation{capability.DropConstraintGeneric: decided(false)},
		wantMet:      false,
	}, {
		name:         "requirement itself undecidable",
		observations: map[capability.Capability]observation{capability.DropConstraintGeneric: cannotDecide("no")},
		wantMet:      false,
	}, {
		name:         "requirement not observed at all",
		observations: map[capability.Capability]observation{},
		wantMet:      false,
	}} {
		c.Run(tc.name, func(c *qt.C) {
			_, met := unmetRequirement(guardExperiment, tc.observations)
			c.Assert(met, qt.Equals, tc.wantMet)
		})
	}
}

// decidedReport builds a report whose every row agrees, so a test can add the
// single defect it is about.
func decidedReport(cell Cell, matched bool) *Report {
	preset := capability.Postgres17()
	report := reportOn(cell, matched, preset)
	report.Planned = true
	report.Control = Attempt{Statement: nonsenseControl}
	report.Resolution.Capabilities = preset
	report.Resolution.VersionSpecific = true

	observations := map[capability.Capability]observation{}
	for _, key := range capability.All() {
		observations[key] = decided(preset.Has(key))
	}
	report.Rows = assemble(report, observations, nil)
	return report
}

func rowFor(c *qt.C, rows []Row, key capability.Capability) Row {
	for _, row := range rows {
		if row.Capability == key {
			return row
		}
	}
	c.Fatalf("no row for capability %q", key)
	return Row{}
}

// assertErrMatches keeps the empty-expectation branch out of the test body.
func assertErrMatches(c *qt.C, err error, want string) {
	checks := map[bool]func(){
		true:  func() { c.Assert(err, qt.IsNil) },
		false: func() { c.Assert(err, qt.ErrorMatches, want) },
	}
	checks[want == ""]()
}
