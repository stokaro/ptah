package mysqldiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
)

func blockSizeChange() *mysqldiff.IndexBlockSize {
	return &mysqldiff.IndexBlockSize{
		Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true},
		After:  &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8},
	}
}

// The codec writes each operand in its model's wire form and decodes it back.
func TestIndexBlockSizeCodec_HappyPath(t *testing.T) {
	c := qt.New(t)
	codec := mysqldiff.IndexBlockSizeCodec()

	encoded, err := codec.Encode(blockSizeChange())

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"before":{"key_block_size":4,"retained":true},"after":{"key_block_size":8}}`)
	c.Assert(must.Must(codec.Decode(encoded)), qt.DeepEquals, schemaext.Payload(blockSizeChange()))
}

// A change needs both operands, a hint the server retains, and two different
// hints; the codec refuses an operand its model refuses.
func TestIndexBlockSize_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, data, wantErr string
	}{
		{"no before", `{"after":{"key_block_size":8}}`, `.*before.*`},
		{"a hint the server discards", `{"before":{"key_block_size":4},"after":{"key_block_size":8}}`,
			`.*requires a hint the server retains.*`},
		{"the same hint", `{"before":{"key_block_size":8,"retained":true},"after":{"key_block_size":8}}`,
			`.*requires two different hints.*`},
		{"an operand its model refuses", `{"before":{"retained":true},"after":{"key_block_size":0}}`, `.*key_block_size.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := mysqldiff.IndexBlockSizeCodec().Decode(json.RawMessage(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// The reversal declares the hint the change replaced, from the hint the change
// declared, retained as it was. Reversing it again is the change.
func TestIndexBlockSizeReversal(t *testing.T) {
	c := qt.New(t)

	reversed, err := mysqldiff.IndexBlockSizeReversal(blockSizeChange())

	c.Assert(err, qt.IsNil)
	c.Assert(reversed, qt.DeepEquals, &mysqldiff.IndexBlockSize{
		Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true},
		After:  &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 4},
	})
	c.Assert(must.Must(mysqldiff.IndexBlockSizeReversal(reversed)), qt.DeepEquals, blockSizeChange())
}

// The safety effect names the table copy a MySQL replacement makes.
func TestIndexBlockSize_Effect(t *testing.T) {
	c := qt.New(t)

	effect := blockSizeChange().Effect()

	c.Assert(effect.Impact, qt.Equals, schemaext.Behavioral)
	c.Assert(effect.Reason, qt.Contains, "ALGORITHM=COPY, which rebuilds the whole table")
}
