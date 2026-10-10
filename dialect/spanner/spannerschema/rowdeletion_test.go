package spannerschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
)

func exact(name string) string { return name }

// TestRowDeletion_RepresentationsAndCodecsRoundTrip pins both representations
// through their codecs, the spelling of each interval kept as written.
func TestRowDeletion_RepresentationsAndCodecsRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		value schemaext.Value
		wire  string
	}{
		{name: "a declaration", codec: 0, value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}},
			wire: `{"column":"created_at","interval":"30 days"}`},
		{name: "an observation", codec: 1, value: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}},
			wire: `{"column":"created_at","interval":"4 WEEKS 2 DAYS"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := spannerschema.Codecs()[test.codec]
			c.Assert(codec.Prototype.Kind(), qt.Equals, spannerschema.RowDeletionKind)
			encoded, err := codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			decoded, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.value)
			c.Assert(test.value.Clone().Equal(test.value), qt.IsTrue)
		})
	}
}

// TestRowDeletion_CodecsRefuseWhatHasNoCanonicalSpelling pins strict decoding:
// both fields are required, null and an unknown or miscased field are
// refused, and a declaration the server refuses does not decode.
func TestRowDeletion_CodecsRefuseWhatHasNoCanonicalSpelling(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "null", data: `null`},
		{name: "no interval", data: `{"column":"ts"}`},
		{name: "a null column", data: `{"column":null,"interval":"1 days"}`},
		{name: "an unknown field", data: `{"column":"ts","interval":"1 days","unit":"SECONDS"}`},
		{name: "a miscased field", data: `{"Column":"ts","interval":"1 days"}`},
		{name: "an interval the server refuses", data: `{"column":"ts","interval":"36 hours"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := spannerschema.Codecs()[0].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

func TestValidateDesired_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   *spannerschema.DesiredRowDeletion
		wantErr string
	}{
		{name: "nil", wantErr: `.*nil Spanner row deletion policy declaration`},
		{name: "no column", value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Interval: "1 days"}}, wantErr: `.*a row deletion policy needs its column`},
		{name: "a quote", value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "1 days'"}},
			wantErr: `.*the interval contains a quote, which the clause cannot carry`},
		{name: "hours", value: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "1 hour"}},
			wantErr: `.*interval "1 hour" is not a whole number of days, which Spanner refuses`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := spannerschema.ValidateDesired(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateObserved_AcceptsWhatTheServerStores pins that an observation is
// not held to the declaration's spellings: a stored interval this owner cannot
// read is compared as text rather than refused.
func TestValidateObserved_AcceptsWhatTheServerStores(t *testing.T) {
	for _, interval := range []string{"4 WEEKS 2 DAYS", "12 MONTHS 5 DAYS", "1 YEAR"} {
		t.Run(interval, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(spannerschema.ValidateObserved(&spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: interval}}), qt.IsNil)
		})
	}
}

// TestEquivalent reads the interval as the hours it denotes, at the server's
// own arithmetic, and the column by the rule it is given.
func TestEquivalent(t *testing.T) {
	tests := []struct {
		name             string
		desired, current spannerschema.Policy
		want             bool
	}{
		{name: "the rewritten interval", desired: spannerschema.Policy{Column: "ts", Interval: "30 days"}, current: spannerschema.Policy{Column: "ts", Interval: "4 WEEKS 2 DAYS"}, want: true},
		{name: "another interval", desired: spannerschema.Policy{Column: "ts", Interval: "31 days"}, current: spannerschema.Policy{Column: "ts", Interval: "4 WEEKS 2 DAYS"}},
		{name: "another column", desired: spannerschema.Policy{Column: "a", Interval: "30 days"}, current: spannerschema.Policy{Column: "b", Interval: "30 days"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(spannerschema.Equivalent(test.desired, test.current, exact), qt.Equals, test.want)
		})
	}
}

// TestRowDeletion_DesiredAndObservedProjectEachOther pins the projections the
// conversion owner uses: each keeps the spelling it was given.
func TestRowDeletion_DesiredAndObservedProjectEachOther(t *testing.T) {
	c := qt.New(t)

	declared := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "30 days"}}
	observed, err := declared.Observed()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Policy, qt.Equals, declared.Policy)
	c.Assert(observed.Desired(), qt.DeepEquals, declared)
	c.Assert((*spannerschema.ObservedRowDeletion)(nil).Desired(), qt.IsNil)
	invalid, err := (&spannerschema.DesiredRowDeletion{}).Observed()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(invalid, qt.IsNil)
}

// TestRowDeletionCoverage_RecordsTheClaimItIsGiven pins coverage: the claim for
// every table, and a subject's own knowledge over it.
func TestRowDeletionCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)
	events := objectidentity.NewBuilder(identifier.ForDialect("spanner")).TableParts("", "events")

	coverage, err := spannerschema.RowDeletionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"},
		[]schemaext.SubjectCoverage{{Kind: spannerschema.RowDeletionKind, Subject: events, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(spannerschema.RowDeletionKind, events).State, qt.Equals, schemaext.Complete)
	other := objectidentity.NewBuilder(identifier.ForDialect("spanner")).TableParts("", "other")
	c.Assert(coverage.Lookup(spannerschema.RowDeletionKind, other).State, qt.Equals, schemaext.Uninspected)
}
