package pgindexstorage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgindexstorage"
)

// TestRecords_TheDefaultSetIsTheOneEverySurfaceCarries pins which parameters
// are recorded under each setting.
//
// The rule is not "what the catalog can report". A parameter recorded on one
// surface and dropped by another makes every such index differ from its own
// inspected document forever, so the recorded set is decided by the weakest
// surface -- which is the Atlas-compatible HCL document, and it has exactly one
// index storage attribute (stokaro/ptah#2183).
func TestRecords_TheDefaultSetIsTheOneEverySurfaceCarries(t *testing.T) {
	tests := []struct {
		name            string
		param           string
		carryEverything bool
		want            bool
	}{
		{name: "pages_per_range by default", param: "pages_per_range", want: true},
		{name: "pages_per_range under the switch", param: "pages_per_range", carryEverything: true, want: true},
		{name: "fillfactor is not recorded by default", param: "fillfactor", want: false},
		{name: "fillfactor is recorded under the switch", param: "fillfactor", carryEverything: true, want: true},
		{name: "an HNSW knob is not recorded by default", param: "m", want: false},
		{name: "an HNSW knob is recorded under the switch", param: "m", carryEverything: true, want: true},
		{name: "ef_construction likewise", param: "ef_construction", carryEverything: true, want: true},
		{name: "a parameter nobody has heard of, under the switch", param: "some_future_knob", carryEverything: true, want: true},
		{name: "and not by default", param: "some_future_knob", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgindexstorage.Records(test.param, test.carryEverything)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// HasCompatibleSlot answers a different question from Records, and the two are
// only the same under the default.
//
// Under the switch a parameter with no compatible slot is still recorded and
// still written -- as a Ptah attribute. Collapsing the two would make the HCL
// writer either announce an omission it did not make, or make one silently.
func TestHasCompatibleSlot_IsAboutTheDocumentRatherThanTheSetting(t *testing.T) {
	tests := []struct {
		name  string
		param string
		want  bool
	}{
		{name: "pages_per_range has an attribute", param: "pages_per_range", want: true},
		{name: "fillfactor has none", param: "fillfactor", want: false},
		{name: "m has none", param: "m", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgindexstorage.HasCompatibleSlot(test.param)

			c.Assert(got, qt.Equals, test.want)
			// Under the switch every one of them is recorded, which is what
			// makes this a separate question rather than the same one.
			c.Assert(pgindexstorage.Records(test.param, true), qt.IsTrue)
		})
	}
}

// The compatible set is returned as a copy, so a caller cannot widen the rule
// for everybody by appending to it.
func TestCompatibleParams_IsACopy(t *testing.T) {
	c := qt.New(t)

	first := pgindexstorage.CompatibleParams()
	c.Assert(first, qt.HasLen, 1)
	first[0] = "tampered"

	c.Assert(pgindexstorage.CompatibleParams(), qt.DeepEquals, []string{"pages_per_range"})
}
