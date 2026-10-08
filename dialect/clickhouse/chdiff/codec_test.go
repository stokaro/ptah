package chdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

func TestTableChangeCodecRetainsIndependentOperands(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}
	after := before.Desired()
	after.PrimaryKey.Value = ""
	value := &chdiff.Table{Before: before, After: after}
	codec := chdiff.Codecs()[0]
	data, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	decoded, err := codec.Decode(data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, value)
	cloned, err := codec.Clone(value)
	c.Assert(err, qt.IsNil)
	canonical, err := codec.Canonical(decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(canonical, qt.DeepEquals, data)
	before.PrimaryKey = "mutated"
	after.OrderBy.Value = "mutated"
	c.Assert(cloned.(*chdiff.Table).Before.PrimaryKey, qt.Equals, "id")
	c.Assert(cloned.(*chdiff.Table).After.OrderBy.Value, qt.Equals, "id")
	c.Assert(decoded.(*chdiff.Table).After.OrderBy.Value, qt.Equals, "id")
	c.Assert(decoded.(*chdiff.Table).After.PrimaryKey, qt.DeepEquals, chschema.Setting{State: chschema.Explicit})
}

func TestTableChangeCodecRefusesMalformedWireData(t *testing.T) {
	for _, data := range []string{
		`null`, `{}`, `{"before":null,"after":{}}`, `{"Before":{},"after":{}}`,
		`{"before":{"engine":"Memory"},"after":{"engine":{"state":"explicit","value":"Memory"}}}`,
		`{"before":{"engine":"Memory","order_by":"","primary_key":"","partition_by":"","sample_by":"","ttl":"","settings":""},"after":{}}`,
		`{"before":{},"after":{},"extra":true}`, `{"before":{},"before":{},"after":{}}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			value, err := chdiff.Codecs()[0].Decode(json.RawMessage(data))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestTableChangeCodecRefusesIncompleteModels(t *testing.T) {
	for _, value := range []schemaext.Payload{
		(*chdiff.Table)(nil), &chdiff.Table{}, &chdiff.Table{Before: &chschema.ObservedTable{Engine: "Memory"}},
		&chdiff.Table{Before: &chschema.ObservedTable{Engine: "Memory"}, After: &chschema.DesiredTable{}},
		&chschema.DesiredTable{},
	} {
		c := qt.New(t)
		codec := chdiff.Codecs()[0]
		cloned, err := codec.Clone(value)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(cloned, qt.IsNil)
		encoded, err := codec.Encode(value)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(encoded, qt.IsNil)
	}
}
