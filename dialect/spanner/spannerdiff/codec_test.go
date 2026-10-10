package spannerdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
)

// TestChangeCodec_RoundTripsEachTransition pins that an omitted operand is a
// known absence on the wire, and that each transition decodes to itself.
func TestChangeCodec_RoundTripsEachTransition(t *testing.T) {
	stored := spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}
	declared := spannerschema.Policy{Column: "created_at", Interval: "60 days"}
	tests := []struct {
		name   string
		change *spannerdiff.RowDeletion
		wire   string
	}{
		{name: "an addition", change: &spannerdiff.RowDeletion{After: &spannerschema.DesiredRowDeletion{Policy: declared}},
			wire: `{"after":{"column":"created_at","interval":"60 days"}}`},
		{name: "a removal", change: &spannerdiff.RowDeletion{Before: &spannerschema.ObservedRowDeletion{Policy: stored}},
			wire: `{"before":{"column":"created_at","interval":"4 WEEKS 2 DAYS"}}`},
		{name: "a change", change: &spannerdiff.RowDeletion{Before: &spannerschema.ObservedRowDeletion{Policy: stored}, After: &spannerschema.DesiredRowDeletion{Policy: declared}},
			wire: `{"after":{"column":"created_at","interval":"60 days"},"before":{"column":"created_at","interval":"4 WEEKS 2 DAYS"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := spannerdiff.Codecs()[0]
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

func TestChangeCodec_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no operand", data: `{}`},
		{name: "a null operand", data: `{"before":null,"after":{"column":"ts","interval":"1 days"}}`},
		{name: "an unknown property", data: `{"after":{"column":"ts","interval":"1 days"},"during":{}}`},
		{name: "an invalid declaration", data: `{"after":{"column":"ts","interval":"36 hours"}}`},
		{name: "null", data: `null`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := spannerdiff.Codecs()[0].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestRowDeletion_CloneChangeSharesNoOperand pins that a cloned change cannot
// reach back into its source through an operand.
func TestRowDeletion_CloneChangeSharesNoOperand(t *testing.T) {
	c := qt.New(t)

	original := &spannerdiff.RowDeletion{
		Before: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "a", Interval: "1 days"}},
		After:  &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "b", Interval: "2 days"}},
	}
	cloned := original.CloneChange().(*spannerdiff.RowDeletion)
	cloned.Before.Policy.Column = "c"
	cloned.After.Policy.Column = "d"

	c.Assert(original.Before.Policy.Column, qt.Equals, "a")
	c.Assert(original.After.Policy.Column, qt.Equals, "b")
	c.Assert((*spannerdiff.RowDeletion)(nil).CloneChange(), qt.DeepEquals, schemaext.ChangeValue((*spannerdiff.RowDeletion)(nil)))
}
