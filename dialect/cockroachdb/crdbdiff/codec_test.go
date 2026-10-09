package crdbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// TestChangeCodec_RoundTripsEachTransition pins that an omitted operand is a
// known absence on the wire, and that each transition decodes to itself.
func TestChangeCodec_RoundTripsEachTransition(t *testing.T) {
	policy := crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(5))}
	tests := []struct {
		name   string
		change *crdbdiff.RowTTL
		wire   string
	}{
		{name: "an addition", change: &crdbdiff.RowTTL{After: &crdbschema.DesiredRowTTL{Policy: policy}},
			wire: `{"after":{"ttl_expiration_expression":"expires_at","ttl_select_batch_size":5}}`},
		{name: "a removal", change: &crdbdiff.RowTTL{Before: &crdbschema.ObservedRowTTL{Policy: policy}},
			wire: `{"before":{"ttl_expiration_expression":"expires_at","ttl_select_batch_size":5}}`},
		{name: "a change", change: &crdbdiff.RowTTL{Before: &crdbschema.ObservedRowTTL{Policy: policy}, After: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}}},
			wire: `{"after":{"ttl_expire_after":"1 day"},"before":{"ttl_expiration_expression":"expires_at","ttl_select_batch_size":5}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := crdbdiff.Codecs()[0]
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
		{name: "a null operand", data: `{"before":null,"after":{"ttl_expire_after":"1 day"}}`},
		{name: "an unknown property", data: `{"after":{"ttl_expire_after":"1 day"},"during":{}}`},
		{name: "an invalid declaration", data: `{"after":{"ttl_job_cron":"@daily"}}`},
		{name: "null", data: `null`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := crdbdiff.Codecs()[0].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestRowTTL_CloneChangeSharesNoOperand pins that a cloned change cannot reach
// back into its source through an operand.
func TestRowTTL_CloneChangeSharesNoOperand(t *testing.T) {
	c := qt.New(t)

	original := &crdbdiff.RowTTL{
		Before: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "a", DeleteRateLimit: new(int64(1))}},
		After:  &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "b"}},
	}
	cloned := original.CloneChange().(*crdbdiff.RowTTL)
	*cloned.Before.Policy.DeleteRateLimit = 2
	cloned.After.Policy.ExpirationExpression = "c"

	c.Assert(*original.Before.Policy.DeleteRateLimit, qt.Equals, int64(1))
	c.Assert(original.After.Policy.ExpirationExpression, qt.Equals, "b")
	c.Assert((*crdbdiff.RowTTL)(nil).CloneChange(), qt.DeepEquals, schemaext.ChangeValue((*crdbdiff.RowTTL)(nil)))
}
