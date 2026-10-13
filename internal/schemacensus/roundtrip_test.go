package schemacensus_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemacensus"
)

// TestRoundTrip_PreservesEveryFixtureExceptWhereRecorded is the preservation
// half of the round trip: every census fixture, compared with its own
// conversion in either direction, plans nothing.
//
// A plan here is a false migration. `ptah schema diff --from a.hcl --to
// a.hcl` showed one for every range with a subtype attribute until
// stokaro/ptah#4483, and each earlier one was found the same way, alone. The
// recorded losses are a ratchet: a new one fails here, and a repaired one
// fails the next test until its entry is removed.
func TestRoundTrip_PreservesEveryFixtureExceptWhereRecorded(t *testing.T) {
	c := qt.New(t)

	unrecorded := unrecordedLosses()

	c.Assert(unrecorded, qt.HasLen, 0,
		qt.Commentf("round trips that plan a change, with no recorded loss saying why:\n%s",
			strings.Join(unrecorded, "\n")))
}

// TestRoundTrip_EveryRecordedLossIsStillMeasured keeps the recorded losses a
// description of the product rather than of its past.
func TestRoundTrip_EveryRecordedLossIsStillMeasured(t *testing.T) {
	c := qt.New(t)

	stale := staleLosses()

	c.Assert(stale, qt.HasLen, 0,
		qt.Commentf("recorded losses the round trip no longer has; remove them:\n%s", strings.Join(stale, "\n")))
}

// TestRoundTrip_EveryComparedFieldReachesThePlanExceptWhereRecorded is the
// discrimination half: an empty plan proves nothing about a field both the
// conversion and the comparison drop, so each compared field is changed on
// the converted side alone and the plan must notice.
func TestRoundTrip_EveryComparedFieldReachesThePlanExceptWhereRecorded(t *testing.T) {
	c := qt.New(t)

	unrecorded := unrecordedUnreached()

	c.Assert(unrecorded, qt.HasLen, 0,
		qt.Commentf("compared fields a change of which plans nothing, with no gap or exemption saying why:\n%s",
			strings.Join(unrecorded, "\n")))
}

// TestRoundTrip_EveryRecordedUnreachedFieldIsStillUnreached is the inverse
// control: a repaired gap, or an exemption that stopped being true, fails
// until its entry goes.
func TestRoundTrip_EveryRecordedUnreachedFieldIsStillUnreached(t *testing.T) {
	c := qt.New(t)

	stale := staleUnreached()

	c.Assert(stale, qt.HasLen, 0,
		qt.Commentf("recorded gaps and exemptions the round trip no longer measures as unreached; remove them:\n%s",
			strings.Join(stale, "\n")))
}

// TestRoundTrip_AFieldIsAGapOrAnExemptionNotBoth refuses an entry recorded as
// a defect to repair and as a decision to keep.
func TestRoundTrip_AFieldIsAGapOrAnExemptionNotBoth(t *testing.T) {
	c := qt.New(t)

	both := recordedAsGapAndExemption()

	c.Assert(both, qt.HasLen, 0,
		qt.Commentf("recorded both as a gap and as an exemption:\n%s", strings.Join(both, "\n")))
}

// TestRoundTrip_EveryUnmeasuredFieldIsRecorded makes a field no fixture lets
// the round trip ask about a decision. A field added to the model without a
// fixture the round trip can use fails here the day it is added.
func TestRoundTrip_EveryUnmeasuredFieldIsRecorded(t *testing.T) {
	c := qt.New(t)

	unrecorded, stale := unmeasuredDisagreement()

	c.Assert(unrecorded, qt.HasLen, 0,
		qt.Commentf("compared fields the round trip cannot ask about, with no entry saying why:\n%s",
			strings.Join(unrecorded, "\n")))
	c.Assert(stale, qt.HasLen, 0,
		qt.Commentf("recorded unmeasured fields the round trip now measures; remove them:\n%s",
			strings.Join(stale, "\n")))
}

// TestRoundTrip_EveryRecordedEntrySaysWhy refuses a blank entry in any of the
// four lists.
func TestRoundTrip_EveryRecordedEntrySaysWhy(t *testing.T) {
	c := qt.New(t)

	blank := roundTripEntriesWithNoReason()

	c.Assert(blank, qt.HasLen, 0,
		qt.Commentf("round-trip entries with no reason:\n%s", strings.Join(blank, "\n")))
}

// TestRoundTrip_MeasuresMostOfTheCorpus is the non-vacuity control. A
// measurement that put no pair in scope, or reached no field, would pass every
// assertion above with lists that describe a broken probe.
func TestRoundTrip_MeasuresMostOfTheCorpus(t *testing.T) {
	c := qt.New(t)

	trip := measuredRoundTrip()

	c.Assert(len(trip.Pairs) > 1000, qt.IsTrue, qt.Commentf("%d pairs in scope", len(trip.Pairs)))
	c.Assert(reachedCount(trip) > 1500, qt.IsTrue, qt.Commentf("%d requirements reached", reachedCount(trip)))
	c.Assert(reachedFieldCount(trip) > 300, qt.IsTrue, qt.Commentf("%d fields reached", reachedFieldCount(trip)))
}

// TestAlternates_FitTheirFields keeps the alternates table from naming a
// field the census does not have, or a value the field cannot hold. Either
// would leave the probe's third change silently undone.
func TestAlternates_FitTheirFields(t *testing.T) {
	c := qt.New(t)

	misfits := alternatesThatDoNotFit()

	c.Assert(misfits, qt.HasLen, 0, qt.Commentf("alternates that do not fit: %s", strings.Join(misfits, ", ")))
	c.Assert(len(schemacensus.Alternates()) > 5, qt.IsTrue)
}

// TestPerturb_ChangesEveryInstanceAndLeavesItsInputAlone is the property the
// second change of the probe rests on, for the kinds it changes.
func TestPerturb_ChangesEveryInstanceAndLeavesItsInputAlone(t *testing.T) {
	c := qt.New(t)
	schema := perturbFixture()

	comments, commentsChanged := schemacensus.Perturb(schema, "schemamodel.Table.Comment")
	starts, startsChanged := schemacensus.Perturb(schema, "schemamodel.Sequence.Start")
	keys, keysChanged := schemacensus.Perturb(schema, "schemamodel.Table.PrimaryKey")

	c.Assert(commentsChanged, qt.IsTrue)
	c.Assert([]string{comments.Tables[0].Comment, comments.Tables[1].Comment}, qt.DeepEquals, []string{"kept_rt", "other_rt"})
	c.Assert(startsChanged, qt.IsTrue)
	c.Assert(*starts.Sequences[0].Start, qt.Equals, int64(8))
	c.Assert(keysChanged, qt.IsTrue)
	c.Assert(keys.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id_rt"})
	c.Assert(schema.Tables[0].Comment, qt.Equals, "kept")
	c.Assert(*schema.Sequences[0].Start, qt.Equals, int64(7))
	c.Assert(schema.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
}

// TestPerturb_LeavesWhatItCannotChange separates "changed" from "left alone":
// a bool, a zero value and a struct are removed by Ablate, not perturbed.
func TestPerturb_LeavesWhatItCannotChange(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "a bool", field: "schemamodel.Sequence.Cycle"},
		{name: "a zero string", field: "schemamodel.Table.Engine"},
		{name: "a struct", field: "pgtable.DesiredPartition.Normalized"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, changed := schemacensus.Perturb(perturbFixture(), test.field)
			c.Assert(changed, qt.IsFalse)
		})
	}
}

// TestAlternate_SetsInstancesInTurn covers the third change: the n-th
// instance takes the n-th value, and a field with no alternate is left alone.
func TestAlternate_SetsInstancesInTurn(t *testing.T) {
	c := qt.New(t)
	schema := schemamodel.Database{
		Indexes: []schemamodel.Index{{Name: "i", TableName: "b"}, {Name: "j", TableName: "a"}},
	}

	moved, changed := schemacensus.Alternate(schema, "schemamodel.Index.TableName")
	_, other := schemacensus.Alternate(schema, "schemamodel.Index.Name")

	c.Assert(changed, qt.IsTrue)
	c.Assert(moved.Indexes[0].TableName, qt.Equals, "a")
	c.Assert(moved.Indexes[1].TableName, qt.Equals, "a")
	c.Assert(schema.Indexes[0].TableName, qt.Equals, "b")
	c.Assert(other, qt.IsFalse)
}

// measuredRoundTrip runs the round trip once for the whole file.
var measuredRoundTrip = sync.OnceValue(func() schemacensus.RoundTrip {
	return must.Must(schemacensus.MeasureRoundTrip(context.Background(), must.Must(builtin.New())))
})

func perturbFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "T", Name: "t", Comment: "kept", PrimaryKey: []string{"id"}},
			{StructName: "U", Name: "u", Comment: "other"},
		},
		Sequences: []schemamodel.Sequence{{Name: "s", Start: new(int64(7))}},
	}
}

// The helpers below select the data each assertion is about. Every caller
// asserts the same way, on the list being empty.

// lossKey names one measured or recorded loss.
func lossKey(fixture, dialect string, direction schemacensus.Direction) string {
	return fixture + "@" + dialect + " " + string(direction)
}

// directionsOf expands a recorded direction list, nil meaning both.
func directionsOf(directions []schemacensus.Direction) []schemacensus.Direction {
	if directions == nil {
		return schemacensus.RoundTripDirections()
	}
	return directions
}

// recordedLosses is every loss the register records.
func recordedLosses() map[string]bool {
	recorded := make(map[string]bool)
	for _, entry := range schemacensus.RoundTripLosses() {
		for _, fixture := range entry.Fixtures {
			for _, dialect := range entry.Dialects {
				for _, direction := range directionsOf(entry.Directions) {
					recorded[lossKey(fixture, dialect, direction)] = true
				}
			}
		}
	}
	return recorded
}

// unrecordedLosses is every measured loss no entry records, with its plan.
func unrecordedLosses() []string {
	recorded := recordedLosses()
	var unrecorded []string
	for _, loss := range measuredRoundTrip().Losses {
		if !recorded[lossKey(loss.Fixture, loss.Dialect, loss.Direction)] {
			unrecorded = append(unrecorded, fmt.Sprintf("%s:\n%s",
				lossKey(loss.Fixture, loss.Dialect, loss.Direction), indent(loss.Plan)))
		}
	}
	slices.Sort(unrecorded)
	return unrecorded
}

// staleLosses is every recorded loss the measurement does not have.
func staleLosses() []string {
	measured := make(map[string]bool)
	for _, loss := range measuredRoundTrip().Losses {
		measured[lossKey(loss.Fixture, loss.Dialect, loss.Direction)] = true
	}
	var stale []string
	for key := range recordedLosses() {
		if !measured[key] {
			stale = append(stale, key)
		}
	}
	slices.Sort(stale)
	return stale
}

// reachKey names one requirement.
func reachKey(field, dialect string, direction schemacensus.Direction) string {
	return field + " on " + dialect + " " + string(direction)
}

// recordedUnreached is every requirement a gap or an exemption records,
// mapped to the list that records it.
func recordedUnreached() map[string][]string {
	recorded := make(map[string][]string)
	for list, entries := range map[string][]schemacensus.Unreached{
		"gap": schemacensus.RoundTripGaps(), "exemption": schemacensus.RoundTripExemptions(),
	} {
		for _, entry := range entries {
			dialects := entry.Dialects
			if dialects == nil {
				dialects = []string{""}
			}
			for _, dialect := range dialects {
				for _, direction := range directionsOf(entry.Directions) {
					key := reachKey(entry.Field, dialect, direction)
					recorded[key] = append(recorded[key], list)
				}
			}
		}
	}
	return recorded
}

// unrecordedUnreached is every unmet requirement nothing records, with where
// the change went.
func unrecordedUnreached() []string {
	recorded := recordedUnreached()
	var unrecorded []string
	for _, reach := range measuredRoundTrip().Reach {
		key := reachKey(reach.Field, reach.Dialect, reach.Direction)
		if reach.Reached || len(recorded[key]) > 0 {
			continue
		}
		detail := "lost in " + reach.Lost
		if reach.Refusal != "" {
			detail += ": " + reach.Refusal
		}
		unrecorded = append(unrecorded, fmt.Sprintf("%s (%s; fixtures %s)", key, detail, strings.Join(reach.Covered, ", ")))
	}
	slices.Sort(unrecorded)
	return unrecorded
}

// staleUnreached is every recorded requirement the measurement does not
// report as unmet.
func staleUnreached() []string {
	unmet := make(map[string]bool)
	for _, reach := range measuredRoundTrip().Reach {
		if !reach.Reached {
			unmet[reachKey(reach.Field, reach.Dialect, reach.Direction)] = true
		}
	}
	var stale []string
	for key, lists := range recordedUnreached() {
		if !unmet[key] {
			stale = append(stale, key+" ("+strings.Join(lists, ", ")+")")
		}
	}
	slices.Sort(stale)
	return stale
}

// recordedAsGapAndExemption is every requirement both lists record.
func recordedAsGapAndExemption() []string {
	var both []string
	for key, lists := range recordedUnreached() {
		if len(lists) > 1 {
			both = append(both, key)
		}
	}
	slices.Sort(both)
	return both
}

// unmeasuredDisagreement compares the measured unmeasured fields that need a
// record with the recorded ones. A field unmeasured only because the pairs
// that read it are lost is the recorded loss's; one no statement reads has no
// requirement to measure.
func unmeasuredDisagreement() (unrecorded, stale []string) {
	recorded := make(map[string]bool)
	for _, entry := range schemacensus.RoundTripUnmeasured() {
		recorded[entry.Field] = true
	}
	needed := make(map[string]bool)
	for _, entry := range measuredRoundTrip().Unmeasured {
		if entry.Why == schemacensus.UnmeasuredLost || entry.Why == schemacensus.UnmeasuredUnread {
			continue
		}
		needed[entry.Field] = true
		if !recorded[entry.Field] {
			unrecorded = append(unrecorded, entry.Field+" ("+entry.Why+")")
		}
	}
	for field := range recorded {
		if !needed[field] {
			stale = append(stale, field)
		}
	}
	slices.Sort(unrecorded)
	slices.Sort(stale)
	return unrecorded, stale
}

// roundTripEntriesWithNoReason is every recorded entry excused in silence.
func roundTripEntriesWithNoReason() []string {
	var blank []string
	for _, entry := range schemacensus.RoundTripLosses() {
		if strings.TrimSpace(entry.Reason) == "" {
			blank = append(blank, "loss "+strings.Join(entry.Fixtures, ","))
		}
	}
	for _, entry := range slices.Concat(schemacensus.RoundTripGaps(), schemacensus.RoundTripExemptions()) {
		if strings.TrimSpace(entry.Reason) == "" {
			blank = append(blank, "unreached "+entry.Field)
		}
	}
	for _, entry := range schemacensus.RoundTripUnmeasured() {
		if strings.TrimSpace(entry.Reason) == "" {
			blank = append(blank, "unmeasured "+entry.Field)
		}
	}
	return blank
}

// alternatesThatDoNotFit is every alternate naming no field, or holding a
// value its field cannot take.
func alternatesThatDoNotFit() []string {
	var misfits []string
	for _, field := range schemacensus.Alternates() {
		if !schemacensus.AlternateFits(field) {
			misfits = append(misfits, field)
		}
	}
	return misfits
}

// reachedCount is how many requirements a change met.
func reachedCount(trip schemacensus.RoundTrip) int {
	count := 0
	for _, reach := range trip.Reach {
		if reach.Reached {
			count++
		}
	}
	return count
}

// reachedFieldCount is how many fields met at least one requirement.
func reachedFieldCount(trip schemacensus.RoundTrip) int {
	fields := make(map[string]bool)
	for _, reach := range trip.Reach {
		if reach.Reached {
			fields[reach.Field] = true
		}
	}
	return len(fields)
}

// indent sets a plan off under the loss it belongs to.
func indent(text string) string {
	return "    " + strings.ReplaceAll(text, "\n", "\n    ")
}
