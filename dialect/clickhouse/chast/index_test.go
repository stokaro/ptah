package chast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
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

// TestAddSkippingIndexDeclaredFacetsStateWhatItRenders pins that the declaration
// read out of an operation is the index the operation builds: an empty type
// renders minmax and zero granularity renders one granule, so both request the
// default rather than leaving a setting the statement fixes unmanaged.
func TestAddSkippingIndexDeclaredFacetsStateWhatItRenders(t *testing.T) {
	for _, test := range []struct {
		name string
		op   chast.AddSkippingIndex
		want chschema.DesiredIndex
	}{
		{name: "both settings left out", op: chast.AddSkippingIndex{Name: "idx", Expression: "a"}, want: chschema.DesiredIndex{
			IndexType: chschema.Setting{State: chschema.Default}, Granularity: chschema.GranularitySetting{State: chschema.Default},
		}},
		{name: "both settings stated", op: chast.AddSkippingIndex{Name: "idx", Expression: "a", IndexType: "set(100)", Granularity: 4}, want: chschema.DesiredIndex{
			IndexType: chschema.Setting{State: chschema.Explicit, Value: "set(100)"}, Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 4},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			facets, err := test.op.DeclaredFacets()
			c.Assert(err, qt.IsNil)
			value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](facets, chschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(*value, qt.Equals, test.want)
			c.Assert(facets.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
		})
	}
}

// TestAddSkippingIndexRefusesAForeignAccessMethod_FailurePath holds a hand-built
// operation to the owner's rule: a PostgreSQL or MySQL access method is no
// ClickHouse index type, and the server would refuse the statement.
func TestAddSkippingIndexRefusesAForeignAccessMethod_FailurePath(t *testing.T) {
	c := qt.New(t)
	op := &chast.AddSkippingIndex{Name: "idx", Expression: "a", IndexType: "GIN"}
	c.Assert(op.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
	facets, err := op.DeclaredFacets()
	c.Assert(err, qt.ErrorMatches, `(?s).*names a PostgreSQL or MySQL access method.*`)
	c.Assert(facets.IsZero(), qt.IsTrue)
}
