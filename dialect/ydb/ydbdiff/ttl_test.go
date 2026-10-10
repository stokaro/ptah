package ydbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTTLCodec_RoundTripsEachTransition pins that an omitted operand is a known
// absence on the wire, and that each transition decodes to itself, the run
// interval of the observed side included.
func TestTTLCodec_RoundTripsEachTransition(t *testing.T) {
	stored := &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}, RunIntervalSeconds: 1800}
	declared := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "SECONDS"}}
	tests := []struct {
		name   string
		change *ydbdiff.TTL
		wire   string
	}{
		{name: "an addition", change: &ydbdiff.TTL{After: declared}, wire: `{"after":{"column":"e","interval":"PT1H","unit":"SECONDS"}}`},
		{name: "a removal", change: &ydbdiff.TTL{Before: stored}, wire: `{"before":{"column":"ts","interval":"P30D","run_interval_seconds":1800}}`},
		{name: "a change", change: &ydbdiff.TTL{Before: stored, After: declared},
			wire: `{"after":{"column":"e","interval":"PT1H","unit":"SECONDS"},"before":{"column":"ts","interval":"P30D","run_interval_seconds":1800}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbdiff.TTLCodec()
			encoded, err := codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			decoded, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.change)
			cloned, err := codec.Clone(test.change)
			c.Assert(err, qt.IsNil)
			c.Assert(cloned, qt.DeepEquals, test.change)
		})
	}
}

func TestTTLCodec_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no operand", data: `{}`},
		{name: "a null operand", data: `{"before":null,"after":{"column":"ts","interval":"P1D"}}`},
		{name: "an unknown property", data: `{"after":{"column":"ts","interval":"P1D"},"during":{}}`},
		{name: "an invalid declaration", data: `{"after":{"column":"ts","interval":"30 days"}}`},
		{name: "null", data: `null`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := ydbdiff.TTLCodec().Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestTTL_CloneChangeSharesNoOperand pins that a cloned change cannot reach
// back into its source through an operand.
func TestTTL_CloneChangeSharesNoOperand(t *testing.T) {
	c := qt.New(t)

	original := &ydbdiff.TTL{
		Before: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "a", Interval: "P1D"}},
		After:  &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "b", Interval: "P2D"}},
	}
	cloned := original.CloneChange().(*ydbdiff.TTL)
	cloned.Before.Policy.Column = "c"
	cloned.After.Policy.Column = "d"

	c.Assert(original.Before.Policy.Column, qt.Equals, "a")
	c.Assert(original.After.Policy.Column, qt.Equals, "b")
	c.Assert((*ydbdiff.TTL)(nil).CloneChange(), qt.DeepEquals, schemaext.ChangeValue((*ydbdiff.TTL)(nil)))
}
