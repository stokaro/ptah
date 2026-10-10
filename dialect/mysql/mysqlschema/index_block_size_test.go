package mysqlschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// Both representations encode the hint they hold, and an observation whether
// the server keeps it, and decode back to an equal value. Zero and false are
// left out, so a declaration of no hint is an empty object.
func TestIndexBlockSizeCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Value
		want  string
	}{
		{name: "a declared hint", codec: mysqlschema.IndexBlockSizeCodecs()[0],
			value: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}, want: `{"key_block_size":8}`},
		{name: "a declaration of none", codec: mysqlschema.IndexBlockSizeCodecs()[0],
			value: &mysqlschema.DesiredIndexBlockSize{}, want: `{}`},
		{name: "a retained hint", codec: mysqlschema.IndexBlockSizeCodecs()[1],
			value: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, want: `{"key_block_size":4,"retained":true}`},
		{name: "a table that discards hints", codec: mysqlschema.IndexBlockSizeCodecs()[1],
			value: &mysqlschema.ObservedIndexBlockSize{}, want: `{}`},
		{name: "the largest MySQL stores", codec: mysqlschema.IndexBlockSizeCodecs()[0],
			value: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 4294967295}, want: `{"key_block_size":4294967295}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := test.codec.Encode(test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)
			decoded := must.Must(test.codec.Decode(encoded))
			c.Assert(decoded.(schemaext.Value).Equal(test.value), qt.IsTrue)
		})
	}
}

// A decoder accepts only what the encoder writes, and a hint above the
// largest one MySQL stores is refused: the server would truncate it.
func TestIndexBlockSizeCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		codec   schemaext.Codec
		data    string
		wantErr string
	}{
		{name: "a zero spelled out", codec: mysqlschema.IndexBlockSizeCodecs()[0], data: `{"key_block_size":0}`, wantErr: `.*key_block_size.*`},
		{name: "a false spelled out", codec: mysqlschema.IndexBlockSizeCodecs()[1], data: `{"retained":false}`, wantErr: `.*retained.*`},
		{name: "a declaration that says retained", codec: mysqlschema.IndexBlockSizeCodecs()[0], data: `{"retained":true}`, wantErr: `.*retained.*`},
		{name: "a null", codec: mysqlschema.IndexBlockSizeCodecs()[1], data: `{"key_block_size":null}`, wantErr: `.*key_block_size.*`},
		{name: "a negative hint", codec: mysqlschema.IndexBlockSizeCodecs()[0], data: `{"key_block_size":-8}`, wantErr: `.*`},
		{name: "above the MySQL limit", codec: mysqlschema.IndexBlockSizeCodecs()[1], data: `{"key_block_size":4294967296}`,
			wantErr: `.*KEY_BLOCK_SIZE 4294967296 exceeds the mysql limit 4294967295.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := test.codec.Decode(json.RawMessage(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// A declaration reads as the observation of an index on a table of the
// default row format: MariaDB keeps the hint there and MySQL does not. The
// observation converts back to the declaration of the hint it holds.
func TestDesiredIndexBlockSize_Observed(t *testing.T) {
	for _, test := range []struct {
		target   string
		retained bool
	}{{"mariadb", true}, {"mysql", false}} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			declared := &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}

			observed, err := declared.Observed(test.target)

			c.Assert(err, qt.IsNil)
			c.Assert(*observed, qt.Equals, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: test.retained})
			c.Assert(observed.Desired().Equal(declared), qt.IsTrue)
		})
	}
}

// IndexBlockSize reads either representation and validates it; a source
// declaring no hint adds no value.
func TestIndexBlockSize(t *testing.T) {
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
		size   uint64
		found  bool
	}{
		{name: "a declaration", facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8)), size: 8, found: true},
		{name: "an observation", facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
			mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true})), size: 4, found: true},
		{name: "a declaration of none adds nothing", facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 0))},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			size, found, err := mysqlschema.IndexBlockSize(test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(size, qt.Equals, test.size)
			c.Assert(found, qt.Equals, test.found)
			c.Assert(test.facets.TargetScope(mysqlschema.IndexBlockSizeKind), qt.HasLen, 0)
		})
	}
}

// A hint above the MySQL limit is refused where it is added and where it is
// read, whichever way it got into the collection.
func TestIndexBlockSize_FailurePath(t *testing.T) {
	c := qt.New(t)

	added, err := mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 4294967296)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(added.IsZero(), qt.IsTrue)

	size, found, err := mysqlschema.IndexBlockSize(must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 4294967296})))
	c.Assert(err, qt.ErrorMatches, `.*KEY_BLOCK_SIZE 4294967296 exceeds the mysql limit 4294967295.*`)
	c.Assert(size, qt.Equals, uint64(0))
	c.Assert(found, qt.IsTrue)
}
