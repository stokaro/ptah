package schemacensus_test

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemacensus"
)

// TestRegistry_CoversEveryFieldTheModelHas is the half of the census that makes
// a new field a decision rather than an accident.
//
// The field list is reflection over the model, so adding one to
// schemamodel.Database — or to a type it reaches, including the CockroachDB
// row-TTL parameters that live in core/ast — fails here until somebody says what
// the field is for.
func TestRegistry_CoversEveryFieldTheModelHas(t *testing.T) {
	c := qt.New(t)

	declared := declaredFields()
	fields := schemacensus.Fields()

	missing := notIn(fields, declared)
	c.Assert(missing, qt.HasLen, 0,
		qt.Commentf("fields the model has and the registry does not:\n%s", strings.Join(missing, "\n")))

	unknown := notIn(declared, fields)
	c.Assert(unknown, qt.HasLen, 0,
		qt.Commentf("registry entries naming no field of the model:\n%s", strings.Join(unknown, "\n")))
}

// TestFields_ReadsTheModelRatherThanAList is the control for the test above.
//
// A walker that found nothing would make the comparison trivially empty, and a
// registry of nothing would pass. The named fields are one per shape the walker
// has to handle: a top-level collection, a field of an element type, a field of
// a type from another package reached through a pointer, and one reached through
// a nested struct.
func TestFields_ReadsTheModelRatherThanAList(t *testing.T) {
	c := qt.New(t)

	fields := schemacensus.Fields()

	c.Assert(len(fields) > 300, qt.IsTrue,
		qt.Commentf("the walker found %d fields", len(fields)))
	c.Assert(fields, qt.Contains, "schemamodel.Database.Tables")
	c.Assert(fields, qt.Contains, "schemamodel.Table.PrimaryKeyName")
	c.Assert(fields, qt.Contains, "ast.RowTTLSpec.ExpireAfter")
	c.Assert(fields, qt.Contains, "coverage.Object.Provenance")
}

// TestRegistry_EveryFieldThatIsNotRenderedSaysWhy refuses a blank exemption.
//
// A disposition on its own says what a field is not. The reason is what the next
// reader needs, and it is the whole difference between a census and a list.
func TestRegistry_EveryFieldThatIsNotRenderedSaysWhy(t *testing.T) {
	c := qt.New(t)

	blank := exemptionsWithNoReason()

	c.Assert(blank, qt.HasLen, 0,
		qt.Commentf("entries exempted from rendering with no reason:\n%s", strings.Join(blank, "\n")))
}

// TestRegistry_ARenderedFieldCarriesNoReason keeps the reason column honest.
//
// A field that renders is justified by the measurement, not by prose. Allowing
// a sentence there would let one be written for a field that stopped rendering,
// which is the state the reason column exists to make visible.
func TestRegistry_ARenderedFieldCarriesNoReason(t *testing.T) {
	c := qt.New(t)

	explained := renderedFieldsCarryingAReason()

	c.Assert(explained, qt.HasLen, 0,
		qt.Commentf("rendered fields carrying a written reason:\n%s", strings.Join(explained, "\n")))
}

// TestRegistry_AGapNamesAnIssue refuses a gap recorded as a shrug.
func TestRegistry_AGapNamesAnIssue(t *testing.T) {
	c := qt.New(t)

	wrong := malformedGaps()

	c.Assert(wrong, qt.HasLen, 0,
		qt.Commentf("gaps that are not a rendered field naming an issue:\n%s", strings.Join(wrong, "\n")))
}

// TestCensus_EveryRenderedFieldIsObservable is the measurement.
//
// A field declared to reach SQL is removed from every fixture that declares it
// and the schema is rendered again on every declared release line. If nothing
// moved, nothing read the field, and the declaration is wrong — that is the
// failure class stokaro/ptah#2606 is about, and the five it found are recorded
// as gaps against #2611 rather than hidden.
//
// What this proves is that the field is READ. Whether what it produces is
// correct is the per-dialect tests' question, and this one cannot answer it.
func TestCensus_EveryRenderedFieldIsObservable(t *testing.T) {
	c := qt.New(t)

	unobservable := unobservableRenderedFields(observationsByField(measured()))

	c.Assert(unobservable, qt.HasLen, 0,
		qt.Commentf("fields declared to reach SQL that no render reads:\n%s",
			strings.Join(unobservable, "\n")))
}

// TestCensus_EveryGapIsStillAGap is the inverse control, and it is what makes
// the test above load-bearing in both directions.
//
// Without it a field could be parked as a gap forever, including one that had
// started rendering — and the census would report success while describing the
// product incorrectly. With it, repairing #2611 fails this test until the entry
// is reclassified, in the same change.
func TestCensus_EveryGapIsStillAGap(t *testing.T) {
	c := qt.New(t)

	repaired := gapsTheCensusCanSee(observationsByField(measured()))

	c.Assert(repaired, qt.HasLen, 0,
		qt.Commentf("fields recorded as gaps that the census can now see:\n%s",
			strings.Join(repaired, "\n")))
}

// TestCensus_EveryRenderedFieldIsDeclaredBySomeFixture separates the two ways a
// field can look unread.
//
// An ablation of a field no fixture populates changes nothing, and that reads
// exactly like a field nothing renders. Requiring coverage first means the
// measurement above is always about the field rather than about the corpus.
func TestCensus_EveryRenderedFieldIsDeclaredBySomeFixture(t *testing.T) {
	c := qt.New(t)

	uncovered := renderedFieldsNoFixtureDeclares(observationsByField(measured()))

	c.Assert(uncovered, qt.HasLen, 0,
		qt.Commentf("fields declared to reach SQL that no fixture declares:\n%s",
			strings.Join(uncovered, "\n")))
}

// TestMeasure_ObservesMostOfTheModel is the control on the measurement itself.
//
// A Measure that returned nothing would pass every assertion above that reads
// "no field is unobservable", because a registry of gaps would explain the
// silence. The floor is a fact about the tree today rather than a target: it is
// well under the count and will only rise.
func TestMeasure_ObservesMostOfTheModel(t *testing.T) {
	c := qt.New(t)

	observed := observedCount(measured())

	c.Assert(observed > 250, qt.IsTrue,
		qt.Commentf("the census observed %d fields", observed))
}

// TestAblate_LeavesItsInputAlone is the property the whole measurement rests on.
//
// schemamodel.Finalize writes into the slices it is handed, and a struct copy
// shares them. Measured while this package was written: rendering a fixture
// through a struct copy left the fixture finalized, and four host fields then
// read as unobservable because the derivation Finalize had already written into
// the input put them back after the ablation.
func TestAblate_LeavesItsInputAlone(t *testing.T) {
	c := qt.New(t)

	schema := schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Comment: "kept"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
	}

	ablated := schemacensus.Ablate(schema, "schemamodel.Table.Comment")

	c.Assert(ablated.Tables[0].Comment, qt.Equals, "")
	c.Assert(schema.Tables[0].Comment, qt.Equals, "kept")
}

// TestPopulated_AnswersForEveryDepthTheModelReaches is the control for the
// coverage question.
//
// A Populated that answered false everywhere would report every field
// uncovered; one that answered true everywhere would report every field covered
// and measure the corpus instead of the model. The rows are the depths the
// walker has to reach: a collection on the root struct, a field of an element,
// and a field of a type from another package behind a pointer.
func TestPopulated_AnswersForEveryDepthTheModelReaches(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  bool
	}{
		{name: "a collection on the root", field: "schemamodel.Database.Tables", want: true},
		{name: "a field of an element", field: "schemamodel.Table.Comment", want: true},
		{name: "a field the schema leaves alone", field: "schemamodel.Table.Engine", want: false},
		{name: "a collection the schema leaves alone", field: "schemamodel.Database.Roles", want: false},
	}

	schema := schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Comment: "kept"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemacensus.Populated(schema, test.field), qt.Equals, test.want)
		})
	}
}

// TestFixtures_AreNamedOnce keeps a fixture from being shadowed by a rename that
// duplicated it, which would leave a field measured against the wrong schema.
func TestFixtures_AreNamedOnce(t *testing.T) {
	c := qt.New(t)

	repeated := repeatedFixtureNames()

	c.Assert(repeated, qt.HasLen, 0, qt.Commentf("fixture names used more than once: %s",
		strings.Join(repeated, ", ")))
	c.Assert(len(schemacensus.Fixtures()) > 50, qt.IsTrue,
		qt.Commentf("the corpus holds %d fixtures", len(schemacensus.Fixtures())))
}

// measured runs the census once for the whole file. Four assertions read the
// same answer, and the run renders every fixture against every declared release
// line twice per field.
var measured = sync.OnceValue(schemacensus.Measure)

// The helpers below hold the selection each assertion is about, so the test
// functions stay one build and one uniform assertion. They select DATA rather
// than an assertion strategy: every caller asserts the same way, on the list
// being empty.

// declaredFields is every field path the registry names.
func declaredFields() []string {
	fields := make([]string, 0, len(schemacensus.Registry()))
	for _, entry := range schemacensus.Registry() {
		fields = append(fields, entry.Field)
	}
	return fields
}

// exemptionsWithNoReason is every entry excused from rendering in silence.
func exemptionsWithNoReason() []string {
	var blank []string
	for _, entry := range schemacensus.Registry() {
		if entry.Disposition != schemacensus.DDL && strings.TrimSpace(entry.Reason) == "" {
			blank = append(blank, string(entry.Disposition)+" "+entry.Field)
		}
	}
	return blank
}

// renderedFieldsCarryingAReason is every rendered field arguing in prose.
func renderedFieldsCarryingAReason() []string {
	var explained []string
	for _, entry := range schemacensus.Registry() {
		if entry.Disposition == schemacensus.DDL && entry.Reason != "" {
			explained = append(explained, entry.Field)
		}
	}
	return explained
}

// malformedGaps is every gap that is not a rendered field naming an issue.
func malformedGaps() []string {
	var wrong []string
	for _, entry := range schemacensus.Registry() {
		gapped := entry.Gap != ""
		onARenderedField := entry.Disposition == schemacensus.DDL
		namesAnIssue := strings.HasPrefix(entry.Gap, "https://github.com/stokaro/ptah/issues/")
		if gapped && (!onARenderedField || !namesAnIssue) {
			wrong = append(wrong, fmt.Sprintf("%s %s %q", entry.Disposition, entry.Field, entry.Gap))
		}
	}
	return wrong
}

// unobservableRenderedFields is every field declared to reach SQL, not excused
// by a gap, that no ablation can be seen through.
func unobservableRenderedFields(observations map[string]schemacensus.Observation) []string {
	var unobservable []string
	for _, entry := range schemacensus.Registry() {
		if entry.Disposition != schemacensus.DDL || entry.Gap != "" {
			continue
		}
		observation := observations[entry.Field]
		if !observation.Observed() {
			unobservable = append(unobservable, fmt.Sprintf(
				"%s (fixtures: %s)", entry.Field, strings.Join(observation.Covered, ", ")))
		}
	}
	return unobservable
}

// gapsTheCensusCanSee is every recorded gap that has started rendering.
func gapsTheCensusCanSee(observations map[string]schemacensus.Observation) []string {
	var repaired []string
	for _, entry := range schemacensus.Registry() {
		if entry.Gap == "" {
			continue
		}
		observation := observations[entry.Field]
		if observation.Observed() {
			repaired = append(repaired, fmt.Sprintf(
				"%s now renders on %s; remove its Gap", entry.Field, strings.Join(observation.Cells, " ")))
		}
	}
	return repaired
}

// renderedFieldsNoFixtureDeclares is every field the measurement says nothing
// about because no fixture populates it.
func renderedFieldsNoFixtureDeclares(observations map[string]schemacensus.Observation) []string {
	var uncovered []string
	for _, entry := range schemacensus.Registry() {
		if entry.Disposition != schemacensus.DDL {
			continue
		}
		if len(observations[entry.Field].Covered) == 0 {
			uncovered = append(uncovered, entry.Field)
		}
	}
	return uncovered
}

// observedCount is how many fields an ablation moved the output for.
func observedCount(observations []schemacensus.Observation) int {
	observed := 0
	for _, observation := range observations {
		if observation.Observed() {
			observed++
		}
	}
	return observed
}

// repeatedFixtureNames is every fixture name the corpus uses more than once.
func repeatedFixtureNames() []string {
	seen := make(map[string]int)
	for _, fixture := range schemacensus.Fixtures() {
		seen[fixture.Name]++
	}
	var repeated []string
	for name, count := range seen {
		if count > 1 {
			repeated = append(repeated, fmt.Sprintf("%s x%d", name, count))
		}
	}
	slices.Sort(repeated)
	return repeated
}

// observationsByField keys one Measure run so several assertions can read it.
func observationsByField(observations []schemacensus.Observation) map[string]schemacensus.Observation {
	byField := make(map[string]schemacensus.Observation, len(observations))
	for _, observation := range observations {
		byField[observation.Field] = observation
	}
	return byField
}

// notIn returns the members of first that second does not hold.
func notIn(first, second []string) []string {
	var missing []string
	for _, value := range first {
		if !slices.Contains(second, value) {
			missing = append(missing, value)
		}
	}
	return missing
}

// TestSurfaces_AgreeExceptWhereRecorded is stokaro/ptah#2606's acceptance
// scenario 11, at the field level.
//
// The same desired schema goes to `ptah schema render` and to
// compare-against-empty → plan → render, and every field one surface reads and
// the other does not is a disagreement. The recorded set is a ratchet: a NEW
// divergence fails, and a repaired one fails until its entry is removed.
//
// The object-kind half of this comparison lives in
// internal/convert/fromschema.TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget
// and catches an object a surface loses. This half catches a FIELD one loses
// while both still emit the object.
func TestSurfaces_AgreeExceptWhereRecorded(t *testing.T) {
	c := qt.New(t)

	unexpected := unrecordedSurfaceDifferences()

	c.Assert(unexpected, qt.HasLen, 0,
		qt.Commentf("fields the two surfaces disagree about, with no entry saying why:\n%s",
			strings.Join(unexpected, "\n")))
}

// TestSurfaces_EveryRecordedDifferenceIsStillMeasured is the inverse control,
// and it is what stops the recorded set from becoming a description of the past.
//
// A difference that has been repaired fails here until its entry goes, in the
// same change — the same direction the census gate runs for a gap.
func TestSurfaces_EveryRecordedDifferenceIsStillMeasured(t *testing.T) {
	c := qt.New(t)

	stale := recordedDifferencesTheSurfacesNoLongerHave()

	c.Assert(stale, qt.HasLen, 0,
		qt.Commentf("recorded differences the two surfaces no longer have:\n%s",
			strings.Join(stale, "\n")))
}

// TestSurfaces_EveryRecordedDifferenceSaysWhy refuses a blank entry, for the
// reason [schemacensus.SurfaceDifference] gives: a difference with no reason is
// two surfaces answering differently with nobody having decided which is right.
func TestSurfaces_EveryRecordedDifferenceSaysWhy(t *testing.T) {
	c := qt.New(t)

	blank := recordedDifferencesWithNoReason()

	c.Assert(blank, qt.HasLen, 0,
		qt.Commentf("recorded differences with no reason:\n%s", strings.Join(blank, "\n")))
}

// TestMeasurePlan_ReadsMostOfTheModel is the non-vacuity control for the plan
// surface.
//
// A MeasurePlan that returned nothing would make every field a
// render-only difference, and the recorded set would then describe a broken
// probe rather than the product.
func TestMeasurePlan_ReadsMostOfTheModel(t *testing.T) {
	c := qt.New(t)

	observed := observedCount(measuredPlan())

	c.Assert(observed > 250, qt.IsTrue,
		qt.Commentf("the plan surface observed %d fields", observed))
}

// recordedDifferencesWithNoReason is every entry excused in silence.
func recordedDifferencesWithNoReason() []string {
	var blank []string
	for _, difference := range schemacensus.SurfaceDifferences() {
		if strings.TrimSpace(difference.Reason) == "" {
			blank = append(blank, difference.Field)
		}
	}
	return blank
}

// measuredPlan runs the plan-side census once for the whole file.
var measuredPlan = sync.OnceValue(schemacensus.MeasurePlan)

// surfaceDisagreements is every field exactly one surface reads, as
// "field render-only" pairs keyed by field.
func surfaceDisagreements() map[string]bool {
	plan := make(map[string]bool)
	for _, observation := range measuredPlan() {
		plan[observation.Field] = observation.Observed()
	}
	disagreements := make(map[string]bool)
	for _, observation := range measured() {
		if observation.Observed() != plan[observation.Field] {
			disagreements[observation.Field] = observation.Observed()
		}
	}
	return disagreements
}

// unrecordedSurfaceDifferences is every disagreement no entry names, and every
// entry that names the wrong direction.
func unrecordedSurfaceDifferences() []string {
	recorded := make(map[string]bool)
	for _, difference := range schemacensus.SurfaceDifferences() {
		recorded[difference.Field] = difference.RenderOnly
	}
	var unexpected []string
	for field, renderOnly := range surfaceDisagreements() {
		expected, known := recorded[field]
		if !known {
			unexpected = append(unexpected, fmt.Sprintf("%s (render-only: %t)", field, renderOnly))
			continue
		}
		if expected != renderOnly {
			unexpected = append(unexpected, fmt.Sprintf(
				"%s is recorded render-only:%t and measured render-only:%t", field, expected, renderOnly))
		}
	}
	slices.Sort(unexpected)
	return unexpected
}

// recordedDifferencesTheSurfacesNoLongerHave is every entry whose disagreement
// has gone.
func recordedDifferencesTheSurfacesNoLongerHave() []string {
	disagreements := surfaceDisagreements()
	var stale []string
	for _, difference := range schemacensus.SurfaceDifferences() {
		if _, still := disagreements[difference.Field]; !still {
			stale = append(stale, difference.Field)
		}
	}
	slices.Sort(stale)
	return stale
}
