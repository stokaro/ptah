package chdiff_test

import (
	"encoding/json"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

func TestIndexChangeCodecPreservesUnsignedOperands(t *testing.T) {
	c := qt.New(t)
	value := &chdiff.Index{
		Before: &chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: math.MaxUint64},
		After:  (&chschema.ObservedIndex{IndexType: "set ( 100 )", Granularity: 1}).Desired(),
	}
	codec := chdiff.IndexCodecs()[0]
	data, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, "18446744073709551615")
	decoded, err := codec.Decode(data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, value)
	canonical, err := codec.Canonical(decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(canonical, qt.DeepEquals, data)
	cloned, err := codec.Clone(value)
	c.Assert(err, qt.IsNil)
	value.Before.Granularity = 5
	value.After.IndexType.Value = "mutated"
	c.Assert(cloned.(*chdiff.Index).Before.Granularity, qt.Equals, uint64(math.MaxUint64))
	c.Assert(cloned.(*chdiff.Index).After.IndexType.Value, qt.Equals, "set ( 100 )")
	c.Assert(decoded.(*chdiff.Index).Before.Granularity, qt.Equals, uint64(math.MaxUint64))
}

func TestIndexChangeCodecRefusesIncompleteAndInvalidOperands(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `{"before":null,"after":{}}`, `{"Before":{},"after":{}}`,
		`{"before":{},"after":{},"extra":true}`, `{"before":{},"before":{},"after":{}}`,
		`{"before":{"index_type":"minmax","granularity":1},"after":{}}`,
		`{"before":{"index_type":"minmax","granularity":1},"after":{"index_type":{"state":"default"},"granularity":{"state":"explicit","value":1}}}`,
		`{"before":{"index_type":"minmax","granularity":18446744073709551616},"after":{}}`,
		`{"before":{"index_type":"minmax","granularity":-1},"after":{}}`,
		`{"before":{"index_type":"minmax","granularity":0},"after":{}}`,
	} {
		c := qt.New(t)
		value, err := chdiff.IndexCodecs()[0].Decode(json.RawMessage(raw))
		c.Assert(err, qt.IsNotNil)
		c.Assert(value, qt.IsNil)
	}
	for _, value := range []schemaext.Payload{
		nil, (*chdiff.Index)(nil), &chdiff.Index{}, &chdiff.Table{},
		&chdiff.Index{Before: &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}, After: &chschema.DesiredIndex{}},
	} {
		c := qt.New(t)
		codec := chdiff.IndexCodecs()[0]
		encoded, err := codec.Encode(value)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(encoded, qt.IsNil)
		cloned, err := codec.Clone(value)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(cloned, qt.IsNil)
	}
}
