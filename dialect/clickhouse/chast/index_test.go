package chast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/engine"
)

func TestIndexCodecPreservesOperandsWithoutBuiltinRuntime(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/index", Codecs: chast.Codecs()})).Codecs()
	for _, op := range []*chast.AddSkippingIndex{
		{Name: "idx", Expression: "tuple(a, lower(b))", IndexType: "bloom_filter(0.01)", Granularity: 4},
		{Name: "idx", Expression: "a"},
	} {
		data, err := registry.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{op})
		c.Assert(err, qt.IsNil)
		decoded, err := registry.Unmarshal(t.Context(), data)
		c.Assert(err, qt.IsNil)
		c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{op})
		decoded[0].(*chast.AddSkippingIndex).Expression = "mutated"
		c.Assert(op.Expression, qt.Not(qt.Equals), "mutated")
		cloned, err := ast.CloneExtensionPayload(op)
		c.Assert(err, qt.IsNil)
		cloned.(*chast.AddSkippingIndex).Name = "mutated"
		c.Assert(op.Name, qt.Equals, "idx")
	}
}

func TestIndexCodecRefusesIncompleteAndInvalidOperands(t *testing.T) {
	codec := chast.Codecs()[1]
	for _, data := range []string{
		`null`, `{}`, `{"name":"idx","expression":"a","index_type":"minmax"}`,
		`{"name":"idx","expression":null,"index_type":"minmax","granularity":1}`,
		`{"name":"idx","expression":"a","index_type":"minmax","granularity":null}`,
		`{"name":"idx","expression":"a","index_type":"minmax","granularity":-1}`,
		`{"name":"idx","expression":"a","index_type":"minmax","granularity":1.5}`,
		`{"name":"idx","expression":"a","index_type":"minmax","granularity":1,"extra":true}`,
		`{"name":"idx","expression":"a","index_type":"minmax","granularity":1,"name":"other"}`,
		`{"name":" ","expression":"a","index_type":"minmax","granularity":1}`,
		`{"name":"idx","expression":" \t","index_type":"minmax","granularity":1}`,
		`{"name":"idx","expression":"a","index_type":" ","granularity":1}`,
		`{"name":"idx","expression":"a\u0000","index_type":"minmax","granularity":1}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := codec.Decode(json.RawMessage(data))
			c.Assert(err, qt.IsNotNil)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	for _, payload := range []schemaext.Payload{nil, (*chast.AddSkippingIndex)(nil), ttlOperation(), &chast.AddSkippingIndex{}} {
		encoded, err := codec.Encode(payload)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(encoded, qt.IsNil)
		cloned, err := codec.Clone(payload)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(cloned, qt.IsNil)
	}
}
