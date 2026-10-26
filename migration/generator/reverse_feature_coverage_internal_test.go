package generator

// White-box testing required: coverage composition installs a synthetic change owner, so the
// structural reversal census includes opaque feature changes on its target.

import (
	"context"
	"encoding/json"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
)

const coverageChangeKind schemaext.Kind = "example.org/reversal/setting"

type coverageChange struct{ Number int }

func (*coverageChange) Kind() schemaext.Kind { return coverageChangeKind }
func (v *coverageChange) CloneChange() schemaext.ChangeValue {
	return &coverageChange{Number: v.Number}
}

type coverageReverser struct{}

func (coverageReverser) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, len(request.Changes))
	for i, change := range request.Changes {
		change.Value = &coverageChange{Number: -change.Value.(*coverageChange).Number}
		result[i] = schemaext.Reversal{Change: change, Strategy: "restore the prior setting"}
	}
	return result, nil
}

func reverseCoverageFeatures() []schemaext.ChangeRecord {
	return []schemaext.ChangeRecord{{Subject: objectidentity.ID{
		Kind: objectidentity.Kind(coverageChangeKind), Name: objectidentity.Part{Source: "setting", Normalized: "setting"},
	}, Value: &coverageChange{Number: 7}}}
}

func reverseCoveragePlan(c *qt.C, diff *difftypes.SchemaDiff, desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(value.(*coverageChange).Number)
	}
	p := engine.Provider{
		ID: "example.org/reversal", Targets: []engine.Target{{Name: "postgres", Preparation: schemapreparation.Identity{}, Creations: schemaprojection.IdentityCreations{}}},
		Codecs: []schemaext.Codec{{Prototype: &coverageChange{}, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(`{"type":"integer"}`),
			Clone: func(value schemaext.Payload) (schemaext.Payload, error) {
				return value.(*coverageChange).CloneChange(), nil
			},
			Encode: encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				number, err := schemaext.DecodeJSON[int](data)
				return &coverageChange{Number: number}, err
			},
		}},
		Reversals: []engine.Reversal{{Target: "postgres", Kinds: []schemaext.Kind{coverageChangeKind}, Service: coverageReverser{}}},
	}
	p.Codecs = append(p.Codecs, coverageObjectCodec(schemaext.Desired), coverageObjectCodec(schemaext.Observed))
	p.Conversions = []engine.Conversion{{Target: "postgres", Kinds: []schemaext.Kind{coverageObjectKind}, Service: coverageObjectConverter{}}}
	runtime, err := engine.New(p)
	c.Assert(err, qt.IsNil)
	return reverseWithRuntimeForTest(c, runtime, diff, desired, current, "postgres")
}

const coverageObjectKind schemaext.Kind = "example.org/reversal/object"

type coverageObjectValue struct{ Number int }

func (*coverageObjectValue) Kind() schemaext.Kind     { return coverageObjectKind }
func (v *coverageObjectValue) Clone() schemaext.Value { return &coverageObjectValue{Number: v.Number} }
func (v *coverageObjectValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*coverageObjectValue)
	return ok && w != nil && v.Number == w.Number
}
func coverageObject(name string, number int) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(coverageObjectKind), Name: objectidentity.Part{Source: name, Normalized: name}}, Value: &coverageObjectValue{Number: number}}
}
func coverageObjectCodec(representation schemaext.Representation) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(value.(*coverageObjectValue).Number)
	}
	return schemaext.Codec{Prototype: &coverageObjectValue{}, Representation: representation, Version: 1, Definition: json.RawMessage(`{"type":"integer"}`), Clone: func(v schemaext.Payload) (schemaext.Payload, error) { return v.(*coverageObjectValue).Clone(), nil }, Encode: encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) {
		n, err := schemaext.DecodeJSON[int](data)
		return &coverageObjectValue{Number: n}, err
	}}
}

type coverageObjectConverter struct{}

func (coverageObjectConverter) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return request.Values, ctx.Err()
}
