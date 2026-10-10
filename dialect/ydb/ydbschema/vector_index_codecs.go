package ydbschema

import (
	_ "embed" // Embed the definition that identifies the vector index wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed vector-index-codecs.json
var vectorIndexDefinition []byte

// VectorIndexWireDefinition returns an independent description of the vector
// index wire model. The definition covers both representations.
func VectorIndexWireDefinition() json.RawMessage { return slices.Clone(vectorIndexDefinition) }

// vectorShape is the strict shape of the settings object, from the JSON tags
// the encoder writes.
var vectorShape = wireShape[VectorSettings]("vector index settings")

// VectorIndexCodecs returns the version-one desired and observed vector index
// codecs, in that order. Each call returns independent definitions. A decoder
// accepts only the spelling the encoder writes: a key in another letter case,
// a null, and an omitted setting written out as empty or zero are refused.
// Every refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue]. The codecs validate representation invariants;
// they claim no server support.
func VectorIndexCodecs() []schemaext.Codec {
	shape := func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, vectorShape)
		return err
	}
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredVectorIndex]{
			Prototype: &DesiredVectorIndex{}, Representation: schemaext.Desired, Version: 1, Definition: VectorIndexWireDefinition(),
			Shape: shape, Validate: ValidateDesiredVectorIndex,
		}.Codec(),
		schemaext.ModelCodec[*ObservedVectorIndex]{
			Prototype: &ObservedVectorIndex{}, Representation: schemaext.Observed, Version: 1, Definition: VectorIndexWireDefinition(),
			Shape: shape, Validate: ValidateObservedVectorIndex,
		}.Codec(),
	}
}

// VectorIndexCoverage records what one source knows about YDB vector index
// settings. knowledge is the claim for every index the source describes;
// subjects override it for individual indexes. A source that can declare the
// settings records complete knowledge, and so does a read, which reads every
// vector index it returns.
func VectorIndexCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range VectorIndexCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == VectorIndexKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: vector index coverage requires a schema representation", schemaext.ErrInvalidValue)
}
