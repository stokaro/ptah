package generator

// White-box testing required: coverage composition installs a synthetic change owner, so the
// structural reversal census includes opaque feature changes on its target.

import (
	"context"
	"encoding/json"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff/difftypes"
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
	runtime, err := engine.New(engine.Provider{
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
	})
	c.Assert(err, qt.IsNil)
	return reverseWithRuntimeForTest(c, runtime, diff, desired, current, "postgres")
}
