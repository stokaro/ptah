package pgpublication

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

const (
	desiredMemberDefinition = `{"type":"object","required":["table"],"additionalProperties":false,"properties":{` +
		`"schema":{"type":"string","minLength":1},"table":{"type":"string","minLength":1},` +
		`"row_filter":{"type":"string","minLength":1},"columns":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}}`
	observedMemberDefinition = `{"type":"object","required":["schema","table"],"additionalProperties":false,"properties":{` +
		`"schema":{"type":"string","minLength":1},"table":{"type":"string","minLength":1},` +
		`"row_filter":{"type":"string","minLength":1},"columns":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}}`
	publishDefinition = `{"type":"array","uniqueItems":true,"items":{"enum":["insert","update","delete","truncate"]}}`
	namesDefinition   = `{"type":"array","minItems":1,"uniqueItems":true,"items":{"type":"string","minLength":1}}`
)

var (
	desiredDefinition = `{"type":"object","required":["publish"],"additionalProperties":false,"properties":{` +
		`"all_tables":{"const":true},"tables":{"type":"array","minItems":1,"items":` + desiredMemberDefinition + `},` +
		`"schemas":` + namesDefinition + `,"publish":{"anyOf":[{"type":"null"},` + publishDefinition + `]},` +
		`"via_partition_root":{"const":true},"generated_columns":{"enum":["none","stored"]},"owner":{"type":"string","minLength":1}}}`
	observedDefinition = `{"type":"object","required":["all_tables","publish","via_partition_root"],"additionalProperties":false,"properties":{` +
		`"all_tables":{"type":"boolean"},"tables":{"type":"array","minItems":1,"items":` + observedMemberDefinition + `},` +
		`"schemas":` + namesDefinition + `,"publish":` + publishDefinition + `,` +
		`"via_partition_root":{"type":"boolean"},"generated_columns":{"enum":["none","stored"]},"owner":{"type":"string","minLength":1}}}`
)

// The wire shapes. A key the encoder leaves out while it is empty is
// NonEmpty.
var (
	desiredShape = schemaext.ObjectShape{Name: "publication",
		Allowed:  []string{"all_tables", "tables", "schemas", "publish", "via_partition_root", "generated_columns", "owner"},
		Required: []string{"publish"}, Nullable: []string{"publish"},
		NonEmpty: []string{"all_tables", "tables", "schemas", "via_partition_root", "generated_columns", "owner"}}
	observedShape = schemaext.ObjectShape{Name: "publication",
		Allowed:  []string{"all_tables", "tables", "schemas", "publish", "via_partition_root", "generated_columns", "owner"},
		Required: []string{"all_tables", "publish", "via_partition_root"},
		NonEmpty: []string{"tables", "schemas", "generated_columns", "owner"}}
	desiredMemberShape = schemaext.ObjectShape{Name: "publication member",
		Allowed: []string{"schema", "table", "row_filter", "columns"}, Required: []string{"table"}, NonEmpty: []string{"schema", "row_filter", "columns"}}
	observedMemberShape = schemaext.ObjectShape{Name: "publication member",
		Allowed: []string{"schema", "table", "row_filter", "columns"}, Required: []string{"schema", "table"}, NonEmpty: []string{"row_filter", "columns"}}
)

// Codecs returns the version-one desired and observed codecs of the
// publication model, in that order. Each call returns independent
// definitions. The codecs validate representation invariants; they resolve no
// defaults and claim no support. A decoder accepts only the spelling the
// encoder writes, so an omitted value written out, such as an empty owner, is
// refused. The codecs write members, schemas and operations sorted and once
// each. Every refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredPublication]{
			Prototype: &DesiredPublication{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredDefinition),
			Shape: shape(desiredShape, desiredMemberShape), Validate: ValidateDesired, Canonical: canonicalDesired,
		}.Codec(),
		schemaext.ModelCodec[*ObservedPublication]{
			Prototype: &ObservedPublication{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedDefinition),
			Shape: shape(observedShape, observedMemberShape), Validate: ValidateObserved, Canonical: canonicalObserved,
		}.Codec(),
	}
}

// shape checks a publication's keys, then each member's: a struct decoder
// alone would accept a member key in another letter case.
func shape(object, member schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, object)
		if err != nil {
			return err
		}
		tables, found := fields["tables"]
		if !found {
			return nil
		}
		var members []json.RawMessage
		if err := json.Unmarshal(tables, &members); err != nil {
			return fmt.Errorf("%w: publication tables: %w", schemaext.ErrInvalidValue, err)
		}
		for _, raw := range members {
			if _, err := schemaext.DecodeObject(raw, member); err != nil {
				return err
			}
		}
		return nil
	}
}

func canonicalDesired(value *DesiredPublication) *DesiredPublication {
	canonical := value.Copy()
	canonical.Tables = nilIfEmpty(sortedMembers(canonical.Tables))
	canonical.Schemas = nilIfEmpty(sortedNames(canonical.Schemas))
	canonical.Publish = canonicalPublish(canonical.Publish)
	return canonical
}

func canonicalObserved(value *ObservedPublication) *ObservedPublication {
	canonical := value.Copy()
	canonical.Tables = nilIfEmpty(sortedMembers(canonical.Tables))
	canonical.Schemas = nilIfEmpty(sortedNames(canonical.Schemas))
	canonical.Publish = canonical.EffectivePublish()
	return canonical
}

func nilIfEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return values
}
