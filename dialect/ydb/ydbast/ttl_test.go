package ydbast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestAlterTTL_CodecRoundTripsAndClonesIndependently(t *testing.T) {
	c := qt.New(t)

	op := &ydbast.AlterTTL{Change: ydbdiff.TTL{
		Before: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}},
		After:  &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P7D"}},
	}}
	codec := ydbast.TTLCodec()
	encoded, err := codec.Encode(op)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"change":{"after":{"column":"ts","interval":"P7D"},"before":{"column":"ts","interval":"P30D"}}}`)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, op)

	cloned := op.CloneExtension().(*ydbast.AlterTTL)
	cloned.Change.After.Policy.Interval = "mutated"
	c.Assert(op.Change.After.Policy.Interval, qt.Equals, "P7D")
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestAlterTTL_CodecFailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no change", data: `{}`},
		{name: "an extra property", data: `{"change":{"after":{"column":"ts","interval":"P1D"}},"table":"t"}`},
		{name: "a change that changes nothing", data: `{"change":{"after":{"column":"ts","interval":"PT720H"},"before":{"column":"ts","interval":"P30D"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := ydbast.TTLCodec().Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
