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
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// probedDialects are the dialects the probe has a statement table for.
var probedDialects = []string{platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse}

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
	registered := make(map[capability.Capability]bool)
	for _, key := range capability.All() {
		registered[key] = true
	}

	for _, dialect := range probedDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			dialectPlan, ok := planFor(dialect)
			c.Assert(ok, qt.IsTrue)

			byExperiment := make(map[capability.Capability]int)
			for _, current := range dialectPlan.experiments {
				c.Check(len(current.decides) > 0, qt.IsTrue, qt.Commentf("an experiment that decides nothing cannot be run"))
				for _, key := range current.decides {
					byExperiment[key]++
				}
			}
			declared := make(map[capability.Capability]int)
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
// declaration. Postgres argues for none: everything it registers, it measures,
// including the three catalog keys added for the Spanner PostgreSQL interface
// -- a PostgreSQL server answers all three, which is what makes them worth
// asking (stokaro/ptah#942).
func TestPlans_DeclareUndecidableOnlyWhereThisFileRecordsWhy(t *testing.T) {
	for _, tc := range []struct {
		dialect string
		want    []capability.Capability
	}{{
		dialect: platform.Postgres,
		// ShowRoutinePrivilege is the first key any plan declares undecidable
		// on PostgreSQL, and the reason is about the PROBE rather than the
		// server: it connects as one account and cannot ask whether a privilege
		// exists without being able to grant it, and an acceptance test cannot
		// separate an unknown privilege from an absent grantee
		// (stokaro/ptah#916).
		want: []capability.Capability{
			capability.DDLInsideTransaction,
			capability.MigrationTimeouts,
			capability.ShowRoutinePrivilege,
			capability.TransactionalDDL,
		},
	}, {
		dialect: platform.MySQL,
		// The three PostgreSQL user-type kinds are declared rather than asked
		// because this server has no spelling of any of the statements: a
		// refusal would be a syntax error about a different question, not an
		// answer about the key (stokaro/ptah#1717).
		want: []capability.Capability{
			capability.CatalogDefaultPrivileges,
			capability.CatalogDependencies,
			capability.CatalogPartitions,
			capability.CatalogRecursiveCTE,
			capability.CatalogRowStatistics,
			capability.CompositeTypes,
			capability.DDLInsideTransaction,
			capability.DomainTypes,
			capability.MigrationTimeouts,
			capability.PostgresCatalogFunctions,
			capability.RangeTypes,
			capability.RoleManagement,
			capability.RowLevelTTL,
			capability.ShowRoutinePrivilege,
			capability.TransactionalDDL,
		},
	}, {
		dialect: platform.MariaDB,
		want: []capability.Capability{
			capability.CatalogDefaultPrivileges,
			capability.CatalogDependencies,
			capability.CatalogPartitions,
			capability.CatalogRecursiveCTE,
			capability.CatalogRowStatistics,
			capability.CompositeTypes,
			capability.DDLInsideTransaction,
			capability.DomainTypes,
			capability.MigrationTimeouts,
			capability.PostgresCatalogFunctions,
			capability.RangeTypes,
			capability.RoleManagement,
			capability.RowLevelTTL,
			capability.ShowRoutinePrivilege,
			capability.TransactionalDDL,
		},
	}, {
		dialect: platform.ClickHouse,
		want: []capability.Capability{
			// ClickHouse has no pg catalogs, so the recursive-catalog-read
			// question cannot be put to it (stokaro/ptah#1811).
			capability.CatalogPartitions,
			capability.CatalogRecursiveCTE,
			capability.CompositeTypes,
			capability.DDLInsideTransaction,
			capability.DomainTypes,
			capability.MigrationTimeouts,
			capability.Procedures,
			capability.RangeTypes,
			capability.ShowRoutinePrivilege,
			capability.TransactionalDDL,
		},
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
		name: "postgres owes four fewer: the probe cannot ask whether a privilege exists, and neither runtime policy nor the transaction wrapper is a statement it can send",
		cell: measuredCell,
		caps: capability.Postgres17(),
		want: registered - 4,
	}, {
		name: "mysql owes fifteen fewer: role_management, row_level_ttl, the five catalog keys, the three user-type kinds and the three runtime properties name surfaces no MySQL path reads or no statement decides",
		cell: Cell{
			Dialect: platform.MySQL, Line: "9.7",
			Preset: capability.MySQL84, PresetName: "MySQL84",
			Refinement: RefinedByVersion,
		},
		caps: capability.MySQL84(),
		want: registered - 15,
	}, {
		name: "mariadb owes fifteen fewer: the three user-type kinds have no MariaDB spelling, the three runtime properties are not statements, neither pg_class nor pg_default_acl is a catalog it has, and sequences is asked now that Ptah renders, reads and plans one",
		cell: Cell{
			Dialect: platform.MariaDB, Line: "10.11",
			Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
			Refinement: RefinedByVersion,
		},
		caps: capability.MariaDB1011(),
		want: registered - 15,
	}, {
		name: "cockroachdb 26.2 owes every row its preset enables a prerequisite for, less the one the probe cannot ask",
		cell: Cell{
			Dialect: platform.CockroachDB, Line: "26.2",
			Preset: capability.CockroachDB26, PresetName: "CockroachDB26",
			Refinement: RefinedByVersion,
		},
		caps: capability.CockroachDB26(),
		want: registered - 4,
	}, {
		name: "cockroachdb 25.4 excludes the guarded drop row whose generic prerequisite is absent",
		cell: Cell{
			Dialect: platform.CockroachDB, Line: "25.4",
			Preset: capability.CockroachDB25, PresetName: "CockroachDB25",
			Refinement: RefinedByVersion,
		},
		caps: capability.CockroachDB25(),
		want: registered - 5,
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
	const key = capability.XMLType // Postgres17 says true.

	for _, tc := range []struct {
		name        string
		obs         observation
		wantOutcome Outcome
		// wantObserved is false for a row the run never decided: such a row must
		// not carry a server answer it never obtained.
		wantObserved   bool
		wantServerDoes bool
		wantReason     string
		wantMismatch   bool
	}{{
		name:           "the server does what the preset says",
		obs:            decided(true),
		wantOutcome:    Agrees,
		wantObserved:   true,
		wantServerDoes: true,
	}, {
		name:         "the server does not, which is a disagreement and not a warning",
		obs:          decided(false),
		wantOutcome:  Disagrees,
		wantObserved: true,
		wantMismatch: true,
	}, {
		name:        "an undecided key stays undecidable and never becomes agreement",
		obs:         cannotDecide("the precondition was refused"),
		wantOutcome: Undecidable,
		wantReason:  "the precondition was refused",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			report := reportOn(measuredCell, true, capability.Postgres17())
			rows := assemble(report, map[capability.Capability]observation{key: tc.obs}, nil)

			row := rowFor(c, rows, key)
			c.Assert(row.Outcome, qt.Equals, tc.wantOutcome)
			c.Assert(row.Observed, qt.Equals, tc.wantObserved)
			c.Assert(row.ServerDoes, qt.Equals, tc.wantServerDoes)
			c.Assert(row.Reason, qt.Equals, tc.wantReason)
			c.Assert(row.Mismatch(), qt.Equals, tc.wantMismatch)
		})
	}
}

// TestAssemble_UndecidableRowsAreNotCountedAsDecided is the counter to the
// cheapest wrong implementation of this harness: reporting every row it could
// not decide as agreeing, so the matrix comes out green.
func TestAssemble_UndecidableRowsAreNotCountedAsDecided(t *testing.T) {
	c := qt.New(t)

	report := reportOn(measuredCell, true, capability.Postgres17())
	observations := make(map[capability.Capability]observation)
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

	advisoryLocks := rowFor(c, report.Rows, capability.AdvisoryLocks)
	c.Assert(advisoryLocks.Outcome, qt.Equals, Undecidable)
	c.Assert(advisoryLocks.Observed, qt.IsTrue)
	c.Assert(advisoryLocks.ServerDoes, qt.IsTrue)
	c.Assert(advisoryLocks.Reason, qt.Contains, "banner substring")

	views := rowFor(c, report.Rows, capability.Views)
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
			assertErrMatches(c, tc.build().Err(), tc.want)
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
			assertErrMatches(c, tc.build().Err(), tc.want)
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

	observations := make(map[capability.Capability]observation)
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
		observations: make(map[capability.Capability]observation),
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

	observations := make(map[capability.Capability]observation)
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

// A versionless line is credited with what it measures. The banner arm's reason
// -- "an observation on one release cannot be credited to this line" -- has no
// referent when the dialect declares one line and no releases to tell apart, so
// applying it there would discard every row a live Spanner endpoint answered
// while naming a distinction that does not exist (stokaro/ptah#942).
func TestLineReason_CreditsAVersionlessLine(t *testing.T) {
	tests := []struct {
		name string
		cell Cell
		want string
	}{
		{
			name: "the versionless spanner line is credited",
			cell: Cell{
				Dialect: platform.Spanner, Line: "0", Versionless: true,
				Preset: capability.SpannerPostgres, PresetName: "SpannerPostgres",
				Refinement: RefinedByBanner,
			},
			want: "",
		},
		{
			// The control, and the reason the flag exists rather than the
			// refinement being widened: the SAME refinement on a dialect that
			// does have releases still withholds credit.
			name: "the same refinement on a versioned line is not",
			cell: Cell{
				Dialect: platform.CockroachDB, Line: "26.2",
				Preset: capability.CockroachDB26, PresetName: "CockroachDB26",
				Refinement: RefinedByBanner,
			},
			want: "an observation on one release cannot be credited to this line",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			reason := lineReason(&Report{Matched: true, Cell: tt.cell, Dialect: tt.cell.Dialect})

			c.Assert(reason, qt.Contains, tt.want)
			c.Assert(reason == "", qt.Equals, tt.want == "")
		})
	}
}

// setupStatements flattens a plan's preconditions in declaration order.
func setupStatements(p plan) []string {
	var out []string
	for _, e := range p.experiments {
		out = append(out, e.setup...)
	}
	return out
}

// The three dialects that were always measured with the PostgreSQL spelling
// keep it, byte for byte. This is the control on the whole change: the Spanner
// spelling exists so those three do not acquire primary keys they never needed,
// and a shared plan is exactly where that would happen unnoticed.
//
// Confirmed live as well: the probe's full report against PostgreSQL 18 is
// identical before and after, down to every statement, differing only in the
// random throwaway namespace (stokaro/ptah#942).
func TestPostgresFamilyPlan_LeavesTheSharedSpellingAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "cockroachdb reads the postgres spelling", dialect: platform.CockroachDB},
		{name: "yugabytedb reads the postgres spelling", dialect: platform.YugabyteDB},
	}

	postgres := setupStatements(postgresFamilyPlan(platform.Postgres))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(setupStatements(postgresFamilyPlan(tt.dialect)), qt.DeepEquals, postgres)
		})
	}

	t.Run("and the statements themselves are unchanged", func(t *testing.T) {
		c := qt.New(t)

		c.Assert(postgres, qt.Contains, "CREATE TABLE dcie (n int)")
		c.Assert(postgres, qt.Contains, "CREATE TABLE dcg (n int, CONSTRAINT dcg_uq UNIQUE (n))")
		c.Assert(postgres, qt.Contains, "CREATE TABLE fkp_uni (k int NOT NULL, CONSTRAINT fkp_uni_uq UNIQUE (k))")
		c.Assert(postgres, qt.Not(qt.Any(qt.Contains)), "PRIMARY KEY (n)")
	})
}

// Spanner reaches Ptah over the PostgreSQL wire and does not speak PostgreSQL
// DDL. Each difference below is a refusal measured against a live endpoint, not
// a precaution.
func TestPostgresFamilyPlan_SpellsSpannerDDL(t *testing.T) {
	spanner := setupStatements(postgresFamilyPlan(platform.Spanner))

	tests := []struct {
		name string
		want string
	}{
		{
			// `Primary key must be defined for table "dcie"`.
			name: "every throwaway table carries a primary key",
			want: "CREATE TABLE dcie (n int, PRIMARY KEY (n))",
		},
		{
			// `<UNIQUE> constraint is not supported, create a unique index
			// instead` -- so the droppable-constraint experiment drops the
			// constraint kind Spanner does have, rather than reporting that
			// Spanner cannot drop a constraint at all.
			name: "the droppable constraint is a CHECK",
			want: "CREATE TABLE dcg (n int, CONSTRAINT dcg_uq CHECK (n > 0), PRIMARY KEY (n))",
		},
		{
			name: "a foreign-key target is made unique by index",
			want: "CREATE UNIQUE INDEX fkp_uni_uq ON fkp_uni (k)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(spanner, qt.Contains, tt.want)
		})
	}
}

// The generic half of the same claim: nothing in the Spanner arm creates a
// table without a key, because one such statement makes every capability the
// experiment decides undecidable -- which is how this started, at ten rows out
// of twenty-eight.
func TestPostgresFamilyPlan_SpannerCreatesNoKeylessTable(t *testing.T) {
	c := qt.New(t)

	for _, statement := range setupStatements(postgresFamilyPlan(platform.Spanner)) {
		keyed := strings.Contains(statement, "PRIMARY KEY") || !strings.HasPrefix(statement, "CREATE TABLE")
		c.Assert(keyed, qt.IsTrue, qt.Commentf("%q creates a table Spanner will refuse", statement))
	}
}

// Entering a throwaway namespace is not evidence that it governs where objects
// land. Measured on the Cloud Spanner emulator through PGAdapter: CREATE SCHEMA
// is accepted, SET search_path is accepted, and an unqualified CREATE TABLE
// lands in `public` regardless -- so every object outlives the run and the next
// run reads them as findings. Two runs against one server answered differently,
// both exiting non-zero for unrelated-looking reasons: nine capability
// disagreements on a fresh server, three on the second (stokaro/ptah#942).
func TestNamespaceProblem(t *testing.T) {
	tests := []struct {
		name        string
		sentinel    int64
		occupants   int64
		wantRefusal string
	}{
		{
			// The ordinary case, and the reason the server's contents are then
			// none of the probe's business: the run cannot see them and they
			// cannot see it.
			name:      "the namespace applies, on a server holding anything at all",
			sentinel:  1,
			occupants: 4000,
		},
		{
			name:      "the namespace applies on an empty server too",
			sentinel:  1,
			occupants: 0,
		},
		{
			// Survivable: the objects land beside nothing.
			name:      "it does not apply and the server is the run's own",
			sentinel:  0,
			occupants: 0,
		},
		{
			name:        "it does not apply and something else is already there",
			sentinel:    0,
			occupants:   23,
			wantRefusal: "already holds 23 table(s)",
		},
		{
			// One leftover is the whole hazard: it is the object the next run
			// would read as its own finding.
			name:        "one leftover is enough",
			sentinel:    0,
			occupants:   1,
			wantRefusal: "already holds 1 table(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := namespaceProblem("ptah_capprobe_test", tt.sentinel, tt.occupants)

			c.Assert(errorMessageOf(err), qt.Contains, tt.wantRefusal)
			c.Assert(err == nil, qt.Equals, tt.wantRefusal == "")
		})
	}
}

// errorMessageOf is "" for a nil error, so one assertion covers both outcomes.
func errorMessageOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The occupancy count must exclude the catalog's own schemas on every dialect
// the probe reaches, or a server with nothing on it counts hundreds of tables
// and the run is refused for the catalog existing.
func TestOccupancySQL_ExcludesEveryCatalogSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{name: "the SQL standard catalog", schema: "information_schema"},
		{name: "PostgreSQL's own", schema: "pg_catalog"},
		{name: "Spanner's own", schema: "spanner_sys"},
		{name: "MySQL's system database", schema: "mysql"},
		{name: "MySQL's instrumentation", schema: "performance_schema"},
		{name: "MySQL's helper views", schema: "sys"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(occupancySQL, qt.Contains, "'"+tt.schema+"'")
		})
	}
}

// A cell may declare that its preset claims LESS than the server does, and the
// probe then reports the difference instead of failing on it. The direction is
// the whole safety property: understating costs a capability, overstating costs
// a migration, and no written reason makes the second acceptable
// (stokaro/ptah#942).
func TestOutcomeFor_HonorsADeclaredUnderstatement(t *testing.T) {
	declared := map[capability.Capability]string{
		capability.Sequences: "a serial column needs a database option Ptah cannot set",
	}

	tests := []struct {
		name        string
		key         capability.Capability
		presetSays  bool
		serverDoes  bool
		understates map[capability.Capability]string
		want        Outcome
	}{
		{
			name: "the declared key, understated", key: capability.Sequences,
			presetSays: false, serverDoes: true, understates: declared, want: Conservative,
		},
		{
			// The direction that must never be silenced: the preset claims a
			// capability the server does not have, which is DDL the server
			// refuses. The declaration names this very key and changes nothing.
			name: "the same key, overstated", key: capability.Sequences,
			presetSays: true, serverDoes: false, understates: declared, want: Disagrees,
		},
		{
			name: "an undeclared key, understated", key: capability.Triggers,
			presetSays: false, serverDoes: true, understates: declared, want: Disagrees,
		},
		{
			name: "no declarations at all", key: capability.Sequences,
			presetSays: false, serverDoes: true, understates: nil, want: Disagrees,
		},
		{
			name: "agreement needs no declaration", key: capability.Sequences,
			presetSays: true, serverDoes: true, understates: declared, want: Agrees,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			row := Row{
				Capability: tt.key, Observed: true,
				PresetSays: tt.presetSays, ServerDoes: tt.serverDoes,
			}

			c.Assert(outcomeFor(row, "", tt.understates), qt.Equals, tt.want)
		})
	}
}

// A conservative row is a difference, not a mismatch: Err() reads Mismatches(),
// so a row that stayed a mismatch would fail the run the declaration exists to
// let pass.
func TestMismatch_ExcludesADeclaredUnderstatement(t *testing.T) {
	tests := []struct {
		name    string
		outcome Outcome
		want    bool
	}{
		{name: "a plain disagreement is a mismatch", outcome: Disagrees, want: true},
		{name: "a declared understatement is not", outcome: Conservative, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			row := Row{
				Capability: capability.Sequences, Observed: true,
				PresetSays: false, ServerDoes: true, Outcome: tt.outcome,
			}

			c.Assert(row.Mismatch(), qt.Equals, tt.want)
		})
	}
}

// Two ClickHouse keys have a statement that LOOKS like them and is accepted:
// the lambda alias for Functions, and the MergeTree TTL clause for RowLevelTTL.
// Asking either would record support for an object the key does not name and no
// Ptah path can carry -- and nothing offline catches the substitution, because
// both spellings compile and only a live server tells them apart. These pin the
// shapes (stokaro/ptah#916).
func TestClickHousePlan_AsksTheShapeTheKeyNames(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wants     []string
		rejects   []string
	}{
		{
			name:      "the function object, not the lambda alias",
			statement: clickHouseFunctionShapeStatement,
			wants:     []string{"RETURNS", "LANGUAGE"},
			rejects:   []string{"->"},
		},
		{
			name:      "the storage parameter, not the MergeTree TTL clause",
			statement: clickHouseTTLShapeStatement(clickHouseSpelling),
			wants:     []string{"ttl_expiration_expression"},
			rejects:   []string{"TTL expires_at +", "TTL d +"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			for _, want := range tt.wants {
				c.Assert(tt.statement, qt.Contains, want)
			}
			for _, reject := range tt.rejects {
				c.Assert(tt.statement, qt.Not(qt.Contains), reject)
			}
		})
	}
}
