package ydbschema

import (
	_ "embed" // Embed the definition that identifies the column storage wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed column-store-codecs.json
var columnStoreDefinition []byte

// ColumnStoreWireDefinition returns an independent description of the column
// storage wire model. The definition covers both representations.
func ColumnStoreWireDefinition() json.RawMessage { return slices.Clone(columnStoreDefinition) }

// The wire shapes come from the JSON tags of the model types, which are what
// the encoder writes, so the keys are spelled in one place.
var (
	storeShape     = wireShape[ColumnStore]("column storage")
	tieredTTLShape = wireShape[TieredTTL]("tiered TTL")
	ttlTierShape   = wireShape[TTLTier]("TTL tier")
)

// ColumnStoreCodecs returns the version-one desired and observed column
// storage codecs, in that order. Each call returns independent definitions.
// Both encode with encoding/json, so equal values encode to the same bytes. A
// decoder accepts only the spelling the encoder writes: a key in another
// letter case, a null, and an omitted value written out, such as an empty
// unit or a zero shard count, are refused. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue]. The
// codecs validate representation invariants; they claim no server support.
func ColumnStoreCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredColumnStore]{
			Prototype: &DesiredColumnStore{}, Representation: schemaext.Desired, Version: 1, Definition: ColumnStoreWireDefinition(),
			Shape: decodeStoreShape, Validate: ValidateDesiredColumnStore,
		}.Codec(),
		schemaext.ModelCodec[*ObservedColumnStore]{
			Prototype: &ObservedColumnStore{}, Representation: schemaext.Observed, Version: 1, Definition: ColumnStoreWireDefinition(),
			Shape: decodeStoreShape, Validate: ValidateObservedColumnStore,
		}.Codec(),
	}
}

// decodeStoreShape checks the value object's keys, the TTL's and each tier's.
// What they hold is the validators'.
func decodeStoreShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, storeShape)
	if err != nil {
		return err
	}
	raw, found := fields["ttl"]
	if !found {
		return nil
	}
	ttl, err := schemaext.DecodeObject(raw, tieredTTLShape)
	if err != nil {
		return err
	}
	tiers, err := schemaext.DecodeJSON[[]json.RawMessage](ttl["tiers"])
	if err != nil {
		return err
	}
	for _, tier := range tiers {
		if _, err := schemaext.DecodeObject(tier, ttlTierShape); err != nil {
			return err
		}
	}
	return nil
}

// ColumnStoreCoverage records what one source knows about YDB column storage.
// knowledge is the claim for every table the source describes; subjects
// override it for individual tables. A desired source that can declare column
// storage records complete knowledge, which makes a table without a value a
// request for row storage. A read records complete knowledge only for the
// tables it returned.
func ColumnStoreCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range ColumnStoreCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == ColumnStoreKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: column storage coverage requires a schema representation", schemaext.ErrInvalidValue)
}
