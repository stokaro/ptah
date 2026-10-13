package pgtable

import (
	"encoding/json"

	"ptah.run/core/schemaext"
)

// ExclusionCodecs returns independent version-one declaration and observation
// codecs. They reject unknown fields, duplicate keys, null and explicit empty
// optional values. Normalization is validated against its source definition.
func ExclusionCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredExclusion]{Prototype: &DesiredExclusion{}, Representation: schemaext.Desired, Version: 1,
			Definition: exclusionDefinition(schemaext.Desired), Shape: desiredExclusionShape, Validate: ValidateDesiredExclusion, Canonical: canonicalDesiredExclusion}.Codec(),
		schemaext.ModelCodec[*ObservedExclusion]{Prototype: &ObservedExclusion{}, Representation: schemaext.Observed, Version: 1,
			Definition: exclusionDefinition(schemaext.Observed), Shape: observedExclusionShape, Validate: ValidateObservedExclusion, Canonical: canonicalObservedExclusion}.Codec(),
	}
}

const (
	exclusionPartDefinition = `{"type":"object","required":["source","normalized"],"additionalProperties":false,"properties":{` +
		`"source":{"type":"string","minLength":1},"normalized":{"type":"string","minLength":1},"quoted":{"const":true},"defaulted":{"const":true}}}`
	exclusionReferenceDefinition = `{"type":"object","required":["kind","name"],"additionalProperties":false,"properties":{` +
		`"kind":{"type":"string","minLength":1},"name":` + exclusionPartDefinition + `,"parent":` + exclusionPartDefinition +
		`,"schema":` + exclusionPartDefinition + `,"catalog":` + exclusionPartDefinition + `,"signature":{"type":"string","minLength":1}}}`
	exclusionProperties = `"using_method":{"type":"string","minLength":1},"elements":{"type":"string","minLength":1},` +
		`"where_condition":{"type":"string","minLength":1},"deferrable":{"const":true},"initially":{"enum":["immediate","deferred"]},` +
		`"comment":{"type":"string","minLength":1},"requires":{"type":"array","minItems":1,"items":` + exclusionReferenceDefinition + `}`
	exclusionNormalizedDefinition = `{"type":"object","required":["source_key","elements"],"additionalProperties":false,"properties":{` +
		`"source_key":{"type":"string","minLength":1},"elements":{"type":"string","minLength":1},"where_condition":{"type":"string","minLength":1}}}`
)

func exclusionDefinition(representation schemaext.Representation) json.RawMessage {
	properties := exclusionProperties
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string","minLength":1},"normalized":` + exclusionNormalizedDefinition
	}
	return json.RawMessage(`{"type":"object","required":["using_method","elements"],"additionalProperties":false,"properties":{` + properties + `}}`)
}

func desiredExclusionShape(data json.RawMessage) error {
	return exclusionShape(data, schemaext.Desired)
}
func observedExclusionShape(data json.RawMessage) error {
	return exclusionShape(data, schemaext.Observed)
}

func exclusionShape(data json.RawMessage, representation schemaext.Representation) error {
	optional := []string{"where_condition", "deferrable", "initially", "comment", "requires"}
	if representation == schemaext.Desired {
		optional = append(optional, "struct_name", "normalized")
	}
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "EXCLUDE constraint", Allowed: append([]string{"using_method", "elements"}, optional...), Required: []string{"using_method", "elements"}, NonEmpty: optional})
	if err != nil {
		return err
	}
	if normalized, ok := fields["normalized"]; ok {
		_, err = schemaext.DecodeObject(normalized, schemaext.ObjectShape{Name: "normalized EXCLUDE", Allowed: []string{"source_key", "elements", "where_condition"},
			Required: []string{"source_key", "elements"}, NonEmpty: []string{"where_condition"}})
		if err != nil {
			return err
		}
	}
	if requires, ok := fields["requires"]; ok {
		return exclusionDependencyShape(requires)
	}
	return nil
}

var exclusionCoverage = schemaext.OwnedCoverageSource(Owner, ExclusionCodecs)

// ExclusionCoverage records knowledge about named EXCLUDE constraints.
// Subject records preserve individual limits without inventing absent objects.
func ExclusionCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return exclusionCoverage(ExclusionKind, representation, knowledge, subjects)
}

func exclusionDependencyShape(data json.RawMessage) error {
	references, err := schemaext.DecodeJSON[[]json.RawMessage](data)
	if err != nil {
		return err
	}
	for _, reference := range references {
		fields, err := schemaext.DecodeObject(reference, schemaext.ObjectShape{Name: "EXCLUDE dependency", Allowed: []string{"kind", "catalog", "schema", "parent", "name", "signature"},
			Required: []string{"kind", "name"}, NonEmpty: []string{"catalog", "schema", "parent", "signature"}})
		if err != nil {
			return err
		}
		for _, component := range []string{"catalog", "schema", "parent", "name"} {
			part, present := fields[component]
			if !present {
				continue
			}
			_, err = schemaext.DecodeObject(part, schemaext.ObjectShape{Name: "EXCLUDE dependency component", Allowed: []string{"source", "normalized", "quoted", "defaulted"},
				Required: []string{"source", "normalized"}, NonEmpty: []string{"source", "normalized", "quoted", "defaulted"}})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func canonicalDesiredExclusion(value *DesiredExclusion) *DesiredExclusion {
	cloned := *value
	cloned.Dependencies = sortedExclusionDependencies(value.Dependencies)
	return &cloned
}

func canonicalObservedExclusion(value *ObservedExclusion) *ObservedExclusion {
	cloned := *value
	cloned.Dependencies = sortedExclusionDependencies(value.Dependencies)
	return &cloned
}
