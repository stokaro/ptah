package ydbschema

import (
	_ "embed" // Embed the definition that identifies the column family wire format.
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

//go:embed column-families-codecs.json
var columnFamiliesDefinition []byte

// ColumnFamiliesWireDefinition returns an independent description of the
// column family wire model. The definition covers both representations.
func ColumnFamiliesWireDefinition() json.RawMessage { return slices.Clone(columnFamiliesDefinition) }

// The wire shapes come from the JSON tags of the model types, which are what
// the encoder writes, so the keys are spelled in one place.
var (
	familiesShape = wireShape[DesiredColumnFamilies]("column families")
	familyShape   = wireShape[ColumnFamily]("column family")
)

// ColumnFamiliesCodecs returns the version-one desired and observed column
// family codecs, in that order. Each call returns independent definitions.
// Both encode with the types' MarshalJSON, so equal values encode to the same
// bytes. A decoder accepts only the spelling the
// encoder writes: a key in another letter case, a null, and an omitted value
// written out, such as an empty data or a false keep_in_memory, are refused.
// Every refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue]. The codecs validate representation invariants;
// they claim no server support.
func ColumnFamiliesCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredColumnFamilies]{
			Prototype: &DesiredColumnFamilies{}, Representation: schemaext.Desired, Version: 1, Definition: ColumnFamiliesWireDefinition(),
			Shape: decodeFamiliesShape, Validate: ValidateDesiredColumnFamilies,
		}.Codec(),
		schemaext.ModelCodec[*ObservedColumnFamilies]{
			Prototype: &ObservedColumnFamilies{}, Representation: schemaext.Observed, Version: 1, Definition: ColumnFamiliesWireDefinition(),
			Shape: decodeFamiliesShape, Validate: ValidateObservedColumnFamilies,
		}.Codec(),
	}
}

// MarshalJSON writes the canonical encoding: the families sorted by name, each
// family's columns sorted, and no family as an empty list rather than null.
// Equal values encode to the same bytes, alone or inside a change or an
// operation. It does not validate; the codecs do.
func (v DesiredColumnFamilies) MarshalJSON() ([]byte, error) {
	type wire DesiredColumnFamilies
	return json.Marshal(wire{Families: canonicalFamilies(v.Families)})
}

// MarshalJSON writes the canonical encoding, as
// [DesiredColumnFamilies.MarshalJSON] does.
func (v ObservedColumnFamilies) MarshalJSON() ([]byte, error) {
	type wire ObservedColumnFamilies
	return json.Marshal(wire{Families: canonicalFamilies(v.Families)})
}

// canonicalFamilies orders an owned copy and spells no family as an empty
// list, which the definition requires.
func canonicalFamilies(families []ColumnFamily) []ColumnFamily {
	sorted := SortedColumnFamilies(families)
	if sorted == nil {
		return make([]ColumnFamily, 0)
	}
	return sorted
}

// decodeFamiliesShape checks the value object's keys and each family's. What
// a family holds is the validators'.
func decodeFamiliesShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, familiesShape)
	if err != nil {
		return err
	}
	families, err := schemaext.DecodeJSON[[]json.RawMessage](fields["families"])
	if err != nil {
		return err
	}
	for _, raw := range families {
		if _, err := schemaext.DecodeObject(raw, familyShape); err != nil {
			return err
		}
	}
	return nil
}

// ColumnFamiliesCoverage records what one source knows about YDB column
// families. knowledge is the claim for every table the source describes;
// subjects override it for individual tables. A desired source that can
// declare families records complete knowledge, which makes a table without a
// value a request for no family of its columns. A read records complete
// knowledge only for the tables it returned, and records a table whose
// families hold a setting it does not model as unrepresentable.
func ColumnFamiliesCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range ColumnFamiliesCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == ColumnFamiliesKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: column family coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// wireShape derives the strict shape of T's JSON object from its tags. Every
// tagged field is allowed. One without omitempty is required. One with
// omitempty is NonEmpty unless it is a pointer: its encoder never writes its
// empty value, while a pointer's false is a value.
func wireShape[T any](name string) schemaext.ObjectShape {
	shape := schemaext.ObjectShape{Name: name}
	for field := range reflect.TypeFor[T]().Fields() {
		key, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		shape.Allowed = append(shape.Allowed, key)
		switch {
		case options != "omitempty":
			shape.Required = append(shape.Required, key)
		case field.Type.Kind() != reflect.Pointer:
			shape.NonEmpty = append(shape.NonEmpty, key)
		}
	}
	return shape
}
