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
	"fmt"
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// probedDialects are the dialects the probe has a statement table for.
var probedDialects = []string{platform.Postgres, platform.MySQL, platform.MariaDB}

// TestPlans_AnswerEveryRegisteredCapabilityExactlyOnce is the guard that keeps
// the matrix complete as the registry grows.
//
// Without it, adding a capability to core/platform/capability would leave every
// dialect silently missing a row while the report still called the run
// complete. "Exactly once" rather than "at least once" is deliberate: two
// experiments deciding the same key would let the later one overwrite the
// earlier answer depending on table order.
//
// The two ways a key can be answered are counted SEPARATELY and only then
// added. Seeding one counter from the declared-undecidable map and adding the
// experiments to it — which is what this test used to do — makes the two
// indistinguishable, so moving a key out of experiments and into undecided
// keeps the total at one and coverage drops with nothing going red. Telling
// them apart is what lets
// TestPlans_DeclareUndecidableOnlyWhereThisFileRecordsWhy hold the split.
func TestPlans_AnswerEveryRegisteredCapabilityExactlyOnce(t *testing.T) {
	registered := map[capability.Capability]bool{}
	for _, key := range capability.All() {
		registered[key] = true
	}

	for _, dialect := range probedDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			dialectPlan, ok := planFor(dialect)
			c.Assert(ok, qt.IsTrue)

			byExperiment := map[capability.Capability]int{}
			for _, current := range dialectPlan.experiments {
				c.Check(len(current.decides) > 0, qt.IsTrue, qt.Commentf("an experiment that decides nothing cannot be run"))
				for _, key := range current.decides {
					byExperiment[key]++
				}
			}
			declared := map[capability.Capability]int{}
			for key := range dialectPlan.undecided {
				declared[key]++
			}

			for key := range registered {
				c.Check(byExperiment[key]+declared[key], qt.Equals, 1,
					qt.Commentf("%s: capability %q is answered %d times by an experiment and %d times by "+
						"declaration, want exactly one answer in total",
						dialect, key, byExperiment[key], declared[key]))
			}
			for key := range byExperiment {
				c.Check(registered[key], qt.IsTrue,
					qt.Commentf("%s: an experiment decides %q, which is not in the capability registry", dialect, key))
			}
			for key := range declared {
				c.Check(registered[key], qt.IsTrue,
					qt.Commentf("%s: the plan declares %q undecidable, which is not in the capability registry", dialect, key))
			}
		})
	}
}

// TestPlans_DeclareUndecidableOnlyWhereThisFileRecordsWhy pins WHICH keys each
// dialect is allowed to answer by declaration instead of by measurement.
//
// Counting answers cannot see this. A key moved from experiments into undecided
// is still answered exactly once, so the count stays at one while the run
// decides one row fewer — the cheapest way to make a stubborn row stop failing
// is to declare it undecidable, and nothing about the totals notices. The
// expected sets below are therefore written out: growing one is a deliberate
// edit to a test, reviewed as the coverage reduction it is, rather than a
// silent side effect of editing plans.go.
//
// Each entry is the set plans.go argues for in a comment at the point of
// declaration. Postgres argues for none: everything it registers, it measures.
func TestPlans_DeclareUndecidableOnlyWhereThisFileRecordsWhy(t *testing.T) {
	for _, tc := range []struct {
		dialect string
		want    []capability.Capability
	}{{
		dialect: platform.Postgres,
		want:    nil,
	}, {
		dialect: platform.MySQL,
		want:    []capability.Capability{capability.RoleManagement},
	}, {
		dialect: platform.MariaDB,
		want:    []capability.Capability{capability.RoleManagement, capability.Sequences},
	}} {
		t.Run(tc.dialect, func(t *testing.T) {
			c := qt.New(t)
			dialectPlan, ok := planFor(tc.dialect)
			c.Assert(ok, qt.IsTrue)
			c.Assert(slices.Sorted(maps.Keys(dialectPlan.undecided)), qt.DeepEquals, tc.want,
				qt.Commentf("%s declares a different set of keys undecidable in advance than this test allows; "+
					"adding one lowers what the probe measures, so it belongs here as a reviewed edit", tc.dialect))
		})
	}
}

func TestIndexIncludeSPGiSTObservation(t *testing.T) {
	accepted := Attempt{Statement: "CREATE INDEX", Accepted: true}
	inspected := Attempt{Statement: "SELECT index metadata", Accepted: true}
	for _, tc := range []struct {
		name      string
		created   Attempt
		inspected Attempt
		matches   int64
		want      observation
	}{
		{
			name:    "create rejection proves the capability false",
			created: Attempt{Statement: "CREATE INDEX", ServerErr: "syntax error"},
			want:    decided(false),
		},
		{
			name:      "metadata failure after acceptance is undecidable",
			created:   accepted,
			inspected: Attempt{Statement: "SELECT index metadata", ServerErr: "catalog unavailable"},
			want: cannotDecide(
				"the index statement was accepted but metadata inspection %q failed (%s), so the run cannot tell "+
					"whether the requested SP-GiST INCLUDE shape was created",
				"SELECT index metadata", "catalog unavailable",
			),
		},
		{
			name:      "exact semantic shape proves the capability true",
			created:   accepted,
			inspected: inspected,
			matches:   1,
			want:      decided(true),
		},
		{
			name:      "accepted but absent semantic shape is false",
			created:   accepted,
			inspected: inspected,
			want: annotated(false,
				"the index statement was accepted but metadata found no SP-GiST index with exactly one key and one "+
					"included column, so the server did not preserve the requested semantics",
			),
		},
		{
			name:      "multiple exact shapes violate the unique-name invariant",
			created:   accepted,
			inspected: inspected,
			matches:   2,
			want: cannotDecide(
				"the index statement was accepted but metadata found %d exact SP-GiST index shapes with one key and one "+
					"included column; more than one match violates the probe's unique-name invariant",
				2,
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := indexIncludeSPGiSTObservation(tc.created, tc.inspected, tc.matches)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestUninspectableIndexIncludeSPGiSTObservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		created Attempt
		want    observation
	}{
		{
			name:    "rejection proves the capability false",
			created: Attempt{Statement: "CREATE INDEX", ServerErr: "syntax error"},
			want:    decided(false),
		},
		{
			name:    "unexpected acceptance is undecidable",
			created: Attempt{Statement: "CREATE INDEX", Accepted: true},
			want: cannotDecide(
				"the index statement was accepted, but this dialect has no portable metadata proof that the payload " +
					"is a non-key included column; syntax acceptance alone does not establish SP-GiST INCLUDE support",
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := uninspectableIndexIncludeSPGiSTObservation(tc.created)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

// TestDecidable_IsDerivedFromThePlanAndTheLine pins the floor's derivation
// against the current registry, plan and preset prerequisites: postgres:17
// owes 25 decisions, mysql:9.7 owes 24, mariadb:10.11 owes 23, and CockroachDB
// 25.4 owes 24 because generic DROP CONSTRAINT is absent there.
func TestDecidable_IsDerivedFromThePlanAndTheLine(t *testing.T) {
	registered := len(capability.All())
	for _, tc := range []struct {
		name string
		cell Cell
		caps capability.Capabilities
		want int
	}{{
		name: "postgres declares nothing undecidable, so every registered row is owed",
		cell: measuredCell,
		caps: capability.Postgres17(),
		want: registered,
	}, {
		name: "mysql owes one fewer: role_management names a surface no MySQL path reads",
		cell: Cell{
			Dialect: platform.MySQL, Line: "9.7",
			Preset: capability.MySQL84, PresetName: "MySQL84",
			Refinement: RefinedByVersion,
		},
		caps: capability.MySQL84(),
		want: registered - 1,
	}, {
		name: "mariadb owes two fewer: sequences is a claim about the generator, not the engine",
		cell: Cell{
			Dialect: platform.MariaDB, Line: "10.11",
			Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
			Refinement: RefinedByVersion,
		},
		caps: capability.MariaDB1011(),
		want: registered - 2,
	}, {
		name: "cockroachdb 26.2 owes every row because its preset enables both experiment prerequisites",
		cell: Cell{
			Dialect: platform.CockroachDB, Line: "26.2",
			Preset: capability.CockroachDB26, PresetName: "CockroachDB26",
			Refinement: RefinedByVersion,
		},
		caps: capability.CockroachDB26(),
		want: registered,
	}, {
		name: "cockroachdb 25.4 excludes the guarded drop row whose generic prerequisite is absent",
		cell: Cell{
			Dialect: platform.CockroachDB, Line: "25.4",
			Preset: capability.CockroachDB25, PresetName: "CockroachDB25",
			Refinement: RefinedByVersion,
		},
		caps: capability.CockroachDB25(),
		want: registered - 1,
	}, {
		name: "a banner-refined line owes nothing because no observation can be credited to it",
		cell: Cell{
			Dialect: platform.YugabyteDB, Line: "2025.2",
			Preset: capability.YugabyteDB25, PresetName: "YugabyteDB25",
			Refinement: RefinedByBanner,
		},
		caps: capability.YugabyteDB25(),
		want: 0,
	}, {
		name: "a line with no measured preset owes nothing either",
		cell: Cell{
			Dialect: platform.MySQL, Line: "26.7",
			Refinement: RefinedByVersion,
			Note:       "no measured MySQL 26 preset",
		},
		caps: capability.MySQL84(),
		want: 0,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dialectPlan, ok := planFor(tc.cell.Dialect)
			c.Assert(ok, qt.IsTrue)
			report := reportOn(tc.cell, true, tc.caps)
			c.Assert(decidable(report, dialectPlan), qt.Equals, tc.want)
		})
	}
}

// TestPlans_DeclaredUndecidablesCarryAReason pins the shape that makes
// undecidable an answer rather than a shrug.
func TestPlans_DeclaredUndecidablesCarryAReason(t *testing.T) {
	for _, dialect := range probedDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			dialectPlan, ok := planFor(dialect)
			c.Assert(ok, qt.IsTrue)
			for key, reason := range dialectPlan.undecided {
				c.Check(len(reason) > 40, qt.IsTrue,
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
			tc.assert(c, rowFor(c.TB, rows, key))
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

// TestAssemble_AnUnattributableLineKeepsTheObservation covers banner-refined
// lines whose preset is not selected by a version and whose release line has
// not been measured directly.
//
// The rows must be undecidable — an observation on one CockroachDB release is
// being credited to every other release, which is not a measurement of this
// line — and the observation must survive anyway, so a contradiction found
// there is reported rather than absorbed by the word UNDECIDABLE.
func TestAssemble_AnUnattributableLineKeepsTheObservation(t *testing.T) {
	c := qt.New(t)

	bannerCell := Cell{
		Dialect: platform.CockroachDB, Line: "25.4",
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
		capability.AdvisoryLocks: decided(true), // CockroachDB23 says false.
		capability.Views:         decided(true), // CockroachDB23 says true.
	}, nil)

	advisoryLocks := rowFor(c.TB, report.Rows, capability.AdvisoryLocks)
	c.Assert(advisoryLocks.Outcome, qt.Equals, Undecidable)
	c.Assert(advisoryLocks.Observed, qt.IsTrue)
	c.Assert(advisoryLocks.ServerDoes, qt.IsTrue)
	c.Assert(advisoryLocks.Reason, qt.Contains, "banner substring")

	views := rowFor(c.TB, report.Rows, capability.Views)
	c.Assert(views.Outcome, qt.Equals, Undecidable)

	c.Assert(report.Mismatches(), qt.HasLen, 1)
	c.Assert(report.Mismatches()[0].Capability, qt.Equals, capability.AdvisoryLocks)
	c.Assert(report.Err(), qt.ErrorMatches, `(?s).*advisory_locks: preset says false, server does true.*`)
}

// TestReportErr covers the ways a run must refuse to report success.
func TestReportErr(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			assertErrMatches(c.TB, tc.build().Err(), tc.want)
		})
	}
}

// TestReportErr_TheFloorIsWhatThePlanPromised covers the erosion a
// "decided at least one row" guard cannot see.
//
// A run that answered one row of twenty-five is not a run that measured this
// server, and before the floor existed it exited zero. The rows below move the
// decided count around a fixed promise, so the boundary is exercised from both
// sides rather than only from the failing one.
func TestReportErr_TheFloorIsWhatThePlanPromised(t *testing.T) {
	registered := len(capability.All())
	for _, tc := range []struct {
		name  string
		build func() *Report
		want  string
	}{{
		name: "a run that decided everything its plan promised passes",
		build: func() *Report {
			return promisedReport(registered)
		},
		want: "",
	}, {
		name: "one promised row short is a failure, not a rounding error",
		build: func() *Report {
			return promisedReport(registered, capability.XMLType)
		},
		want: fmt.Sprintf(
			`(?s).*decided %d of %d capability rows, 1 fewer than the %d the postgres plan promised to answer.*`,
			registered-1, registered, registered,
		),
	}, {
		name: "the shape the old floor let through: one row decided out of twenty-five",
		build: func() *Report {
			return promisedReport(registered, everyKeyExcept(capability.XMLType)...)
		},
		want: fmt.Sprintf(
			`(?s).*decided 1 of %d capability rows, %d fewer than the %d the postgres plan promised to answer.*`,
			registered, registered-1, registered,
		),
	}, {
		name: "a plan that promised nothing still may not decide nothing",
		build: func() *Report {
			return promisedReport(0, everyKeyExcept()...)
		},
		want: fmt.Sprintf(
			`(?s).*decided 0 of %d capability rows; a probe that measured nothing.*`,
			registered,
		),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			assertErrMatches(c.TB, tc.build().Err(), tc.want)
		})
	}
}

// promisedReport builds a report on an attributable, measured line that
// promised to decide `promised` rows and left `undecided` of them unanswered.
func promisedReport(promised int, undecided ...capability.Capability) *Report {
	preset := capability.Postgres17()
	report := reportOn(measuredCell, true, preset)
	report.Planned = true
	report.Control = Attempt{Statement: nonsenseControl}
	report.Resolution.Capabilities = preset
	report.Resolution.VersionSpecific = true
	report.Decidable = promised

	observations := map[capability.Capability]observation{}
	for _, key := range capability.All() {
		observations[key] = decided(preset.Has(key))
	}
	for _, key := range undecided {
		observations[key] = cannotDecide("the deciding statement for %q was never executed", key)
	}
	report.Rows = assemble(report, observations, nil)
	return report
}

// everyKeyExcept returns the registry minus the named keys.
func everyKeyExcept(keep ...capability.Capability) []capability.Capability {
	var out []capability.Capability
	for _, key := range capability.All() {
		if !slices.Contains(keep, key) {
			out = append(out, key)
		}
	}
	return out
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
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
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

func rowFor(tb testing.TB, rows []Row, key capability.Capability) Row {
	c := qt.New(tb)
	for _, row := range rows {
		if row.Capability == key {
			return row
		}
	}
	c.Fatalf("no row for capability %q", key)
	return Row{}
}

// assertErrMatches keeps the empty-expectation branch out of the test body.
func assertErrMatches(tb testing.TB, err error, want string) {
	c := qt.New(tb)
	checks := map[bool]func(){
		true:  func() { c.Assert(err, qt.IsNil) },
		false: func() { c.Assert(err, qt.ErrorMatches, want) },
	}
	checks[want == ""]()
}
