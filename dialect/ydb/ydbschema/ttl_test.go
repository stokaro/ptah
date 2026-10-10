package ydbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTTL_RepresentationsAndCodecsRoundTrip pins both representations through
// their codecs: a unit only for an integer column, and the run interval only on
// an observation that has one.
func TestTTL_RepresentationsAndCodecsRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		value schemaext.Value
		wire  string
	}{
		{name: "a declaration on a date column", codec: 0, value: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "PT720H"}},
			wire: `{"column":"created_at","interval":"PT720H"}`},
		{name: "a declaration on an integer column", codec: 0, value: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "SECONDS"}},
			wire: `{"column":"expires","interval":"PT1H","unit":"SECONDS"}`},
		{name: "an observation with a run interval", codec: 1, value: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}, RunIntervalSeconds: 1800},
			wire: `{"column":"created_at","interval":"P30D","run_interval_seconds":1800}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbschema.TTLCodecs()[test.codec]
			c.Assert(codec.Prototype.Kind(), qt.Equals, ydbschema.TTLKind)
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

// TestTTL_CodecsRefuseWhatHasNoCanonicalSpelling pins strict decoding: an
// omitted field is written by omission, a run interval belongs to an
// observation, and a declaration YDB refuses does not decode.
func TestTTL_CodecsRefuseWhatHasNoCanonicalSpelling(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		data  string
	}{
		{name: "null", codec: 0, data: `null`},
		{name: "no interval", codec: 0, data: `{"column":"ts"}`},
		{name: "an empty unit", codec: 0, data: `{"column":"ts","interval":"P1D","unit":""}`},
		{name: "a unit in lower case", codec: 0, data: `{"column":"ts","interval":"P1D","unit":"seconds"}`},
		{name: "a run interval on a declaration", codec: 0, data: `{"column":"ts","interval":"P1D","run_interval_seconds":60}`},
		{name: "a zero run interval", codec: 1, data: `{"column":"ts","interval":"P1D","run_interval_seconds":0}`},
		{name: "a miscased field", codec: 0, data: `{"Column":"ts","interval":"P1D"}`},
		{name: "Spanner's spelling", codec: 0, data: `{"column":"ts","interval":"30 days"}`},
		{name: "a fraction of a second", codec: 0, data: `{"column":"ts","interval":"PT1.5S"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := ydbschema.TTLCodecs()[test.codec].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestEquivalentTTL reads the interval as the whole seconds YDB keeps, the unit
// exactly, and the column by the rule it is given.
func TestEquivalentTTL(t *testing.T) {
	exact := func(name string) string { return name }
	tests := []struct {
		name string
		a, b ydbschema.TTL
		want bool
	}{
		{name: "the seconds YDB keeps", a: ydbschema.TTL{Column: "ts", Interval: "PT720H"}, b: ydbschema.TTL{Column: "ts", Interval: "P30D"}, want: true},
		{name: "a week", a: ydbschema.TTL{Column: "ts", Interval: "P1W"}, b: ydbschema.TTL{Column: "ts", Interval: "P7D"}, want: true},
		{name: "another interval", a: ydbschema.TTL{Column: "ts", Interval: "P1D"}, b: ydbschema.TTL{Column: "ts", Interval: "P2D"}},
		{name: "another unit", a: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "SECONDS"}, b: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "MILLISECONDS"}},
		{name: "another column", a: ydbschema.TTL{Column: "ts", Interval: "P1D"}, b: ydbschema.TTL{Column: "TS", Interval: "P1D"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbschema.EquivalentTTL(test.a, test.b, exact), qt.Equals, test.want)
			c.Assert(ydbschema.EquivalentTTL(test.b, test.a, exact), qt.Equals, test.want)
		})
	}
}

// TestTTL_DesiredAndObservedProjectEachOther pins the projections the
// conversion owner uses: a declaration becomes what YDB shows of it, and an
// observation becomes a declaration without its run interval.
func TestTTL_DesiredAndObservedProjectEachOther(t *testing.T) {
	c := qt.New(t)

	declared := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "PT720H"}}
	observed, err := declared.Observed()
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.DeepEquals, &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}})
	withRun := &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}, RunIntervalSeconds: 60}
	c.Assert(withRun.Desired(), qt.DeepEquals, &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}})
	c.Assert((*ydbschema.ObservedTTL)(nil).Desired(), qt.IsNil)
	invalid, err := (&ydbschema.DesiredTTL{}).Observed()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(invalid, qt.IsNil)
}

// TestTTLColumnRefusal holds the column a TTL reads to the table's columns and
// to the types YDB reads a TTL from.
func TestTTLColumnRefusal(t *testing.T) {
	types := map[string]string{"ts": "Timestamp", "e": "Uint64", "n": "Int64"}
	tests := []struct {
		name   string
		policy ydbschema.TTL
		want   string
	}{
		{name: "a date column", policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}},
		{name: "an integer column with its unit", policy: ydbschema.TTL{Column: "e", Interval: "P1D", Unit: "SECONDS"}},
		{name: "a column the table does not declare", policy: ydbschema.TTL{Column: "gone", Interval: "P1D"},
			want: "it reads column \"gone\", which the table does not declare (`Cannot enable TTL on unknown column`)"},
		{name: "a signed integer column", policy: ydbschema.TTL{Column: "n", Interval: "P1D", Unit: "SECONDS"},
			want: "column \"n\" is Int64, and YDB reads a TTL from a Date, Datetime, Timestamp, Date32, Datetime64 or Timestamp64 column, " +
				"or from a Uint32, Uint64 or DyNumber column with a unit (`Unsupported column type`)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbschema.TTLColumnRefusal(test.policy, types), qt.Equals, test.want)
		})
	}
}

// TestTTLCoverage_RecordsTheClaimItIsGiven pins coverage: the claim for every
// table, and a subject's own knowledge over it.
func TestTTLCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))

	coverage, err := ydbschema.TTLCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: builder.TableParts("app", "events"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(ydbschema.TTLKind, builder.TableParts("app", "events")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(ydbschema.TTLKind, builder.TableParts("app", "other")).State, qt.Equals, schemaext.Uninspected)
}
