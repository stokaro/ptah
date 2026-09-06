package embedreport

// White-box testing required: the ratchet compares the withheld map, which is
// unexported and must stay so -- a public list of what is not reported is a
// list somebody would read as a feature and reach for.

import (
	"reflect"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
)

// TestStatusOf_EveryStoredFieldIsReportedOrWithheld is the ratchet.
//
// A field added to the stored run must be decided about: reported here, or
// named in withheld with the reason. Neither default is safe. A field that
// arrives on the agent surface because nobody looked is how a row identity
// leaks; a field that stays off it because nobody looked is how an operator
// stops being able to see why their run is stuck.
func TestStatusOf_EveryStoredFieldIsReportedOrWithheld(t *testing.T) {
	c := qt.New(t)
	reported := fieldNames(reflect.TypeFor[Status]())
	// The status renames two fields for its own readers, and the rename is
	// deliberate: a lease has a holder rather than an owner on this surface,
	// and a run's Status field would be status.status in JSON.
	reported["LeaseOwner"] = true
	reported["LeaseExpires"] = true
	reported["ID"] = true
	reported["GenerationIdentity"] = true
	reported["Status"] = true

	for name := range fieldNames(reflect.TypeFor[embedrun.Run]()) {
		_, isWithheld := withheld[name]
		c.Assert(reported[name] || isWithheld, qt.IsTrue,
			qt.Commentf("embedrun.Run.%s is neither reported by Status nor named in withheld; "+
				"decide which, and write the reason", name))
	}
}

// TestWithheld_NamesOnlyFieldsThatExist is the other direction.
//
// A withheld entry for a field that has been renamed or removed is a reason
// nobody is applying, and it makes the ratchet above pass for the wrong
// reason: the entry covers nothing, and the field it was written about is now
// covered by neither half.
func TestWithheld_NamesOnlyFieldsThatExist(t *testing.T) {
	c := qt.New(t)
	stored := fieldNames(reflect.TypeFor[embedrun.Run]())
	for name := range withheld {
		c.Assert(stored[name], qt.IsTrue,
			qt.Commentf("withheld names %s, which embedrun.Run does not have", name))
	}
}

// TestWithheld_EveryReasonSaysSomething refuses an empty one.
func TestWithheld_EveryReasonSaysSomething(t *testing.T) {
	c := qt.New(t)
	for name, reason := range withheld {
		c.Assert(len(reason) > 20, qt.IsTrue,
			qt.Commentf("%s is withheld for %q, which is not a reason", name, reason))
	}
}

// fieldNames returns a struct's field names.
//
// Every field, not only the exported ones: a ratchet that skipped the
// unexported half would let a stored run grow a field carrying content and
// call it decided.
func fieldNames(structType reflect.Type) map[string]bool {
	names := make(map[string]bool, structType.NumField())
	for field := range structType.Fields() {
		names[field.Name] = true
	}
	return names
}

// TestStatusOf_EveryReportedFieldIsActuallyFilled is the ratchet's other half.
//
// The ratchet above compares field NAMES, so a field declared on the status
// and never populated satisfies it: the name is there, the value never
// arrives, and an operator reading "rollback_eligible: false" is reading a Go
// zero rather than an answer. Measured -- with the assignment of ActivePointer
// deleted, the name ratchet stayed green and this one reddens.
//
// The run is filled with distinct non-zero values so a field copied from the
// wrong source is caught too, which a fixture of all-ones could not do.
func TestStatusOf_EveryReportedFieldIsActuallyFilled(t *testing.T) {
	c := qt.New(t)
	run := embedrun.Run{
		ID: "run-1", SpecDigest: "spec-1", GenerationIdentity: "gen-1",
		Environment: "staging", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "local", ResolvedModel: "bge-small-en", PtahVersion: "0.0.0",
		PolicyDigest: "policy-1",
		Phase:        embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-1", LeaseExpires: time.Unix(1700000000, 0).UTC(), FencingToken: 7,
		SnapshotWatermark: "100", CatchUpWatermark: "200", SnapshotDone: true,
		Cursor: []string{"41"},
		Progress: embedrun.Progress{
			RowsScanned: 1, RowsEmbedded: 2, RowsSkipped: 3, RowsDeleted: 4,
			BatchesCommitted: 5, ProviderPromptTokens: 6, ProviderTotalTokens: 8,
			RetryCount: 9,
		},
		VerificationRef: "verify-1", CutoverPlanRef: "plan-1", ApprovalRef: "approval-1",
		ActivePointer: "gen-0", RollbackEligible: true,
		FailureClass: "provider", FailureDetail: "the endpoint refused",
		CreatedAt: time.Unix(1600000000, 0).UTC(), UpdatedAt: time.Unix(1700000001, 0).UTC(),
	}

	status := reflect.ValueOf(StatusOf(run, embedcatchup.ModeOutbox))
	for index := range status.NumField() {
		field := status.Type().Field(index)
		c.Assert(status.Field(index).IsZero(), qt.IsFalse,
			qt.Commentf("Status.%s is declared and never filled from a run that has "+
				"everything", field.Name))
	}
}
