package crdbast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

func TestAlterRowTTL_CodecRoundTripsAndClonesIndependently(t *testing.T) {
	c := qt.New(t)

	op := &crdbast.AlterRowTTL{Change: crdbdiff.RowTTL{
		Before: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "a", Pause: true}},
		After:  &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "b"}},
	}}
	codec := crdbast.Codecs()[0]
	encoded, err := codec.Encode(op)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"change":{"after":{"ttl_expiration_expression":"b"},"before":{"ttl_expiration_expression":"a","ttl_pause":true}}}`)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, op)

	cloned := op.CloneExtension().(*crdbast.AlterRowTTL)
	cloned.Change.After.Policy.ExpirationExpression = "mutated"
	c.Assert(op.Change.After.Policy.ExpirationExpression, qt.Equals, "b")
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestAlterRowTTL_CodecFailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no change", data: `{}`},
		{name: "an extra property", data: `{"change":{"after":{"ttl_expire_after":"1 day"}},"table":"t"}`},
		{name: "a change that changes nothing", data: `{"change":{"after":{"ttl_expire_after":"72 hours"},"before":{"ttl_expire_after":"72:00:00"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := crdbast.Codecs()[0].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
