package spannerast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
)

func TestAlterRowDeletion_CodecRoundTripsAndClonesIndependently(t *testing.T) {
	c := qt.New(t)

	op := &spannerast.AlterRowDeletion{Change: spannerdiff.RowDeletion{
		Before: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}},
		After:  &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "60 days"}},
	}}
	codec := spannerast.Codecs()[0]
	encoded, err := codec.Encode(op)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"change":{"after":{"column":"created_at","interval":"60 days"},"before":{"column":"created_at","interval":"4 WEEKS 2 DAYS"}}}`)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, op)

	cloned := op.CloneExtension().(*spannerast.AlterRowDeletion)
	cloned.Change.After.Policy.Interval = "mutated"
	c.Assert(op.Change.After.Policy.Interval, qt.Equals, "60 days")
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestAlterRowDeletion_CodecFailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no change", data: `{}`},
		{name: "an extra property", data: `{"change":{"after":{"column":"ts","interval":"1 days"}},"table":"t"}`},
		{name: "a change that changes nothing", data: `{"change":{"after":{"column":"ts","interval":"30 days"},"before":{"column":"ts","interval":"4 WEEKS 2 DAYS"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := spannerast.Codecs()[0].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
