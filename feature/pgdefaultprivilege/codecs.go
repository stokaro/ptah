package pgdefaultprivilege

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

const grantDefinition = `{"type":"object","required":["privilege"],"additionalProperties":false,"properties":{` +
	`"privilege":{"type":"string","minLength":1},"with_option":{"const":true}}}`
const listDefinition = `"privileges":{"type":"array","minItems":1,"items":` + grantDefinition + `},` +
	`"revoked":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}`
const desiredDefinition = `{"type":"object","additionalProperties":false,"properties":{` + listDefinition +
	`,"comment":{"type":"string","minLength":1}}}`
const observedDefinition = `{"type":"object","additionalProperties":false,"properties":{` + listDefinition + `}}`

// Codecs returns independent version-one desired and observed codecs, in that
// order. Encoding canonicalizes privilege sets without mutating the input.
// Decoding refuses unknown and duplicate keys, null values, and explicitly empty
// optional fields, including nested grant fields. InvalidModelError wraps each
// refusal and identifies the model and representation.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*Desired]{Prototype: &Desired{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(desiredDefinition), Shape: desiredShape, Validate: ValidateDesired, Canonical: canonicalDesired}.Codec(),
		schemaext.ModelCodec[*Observed]{Prototype: &Observed{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(observedDefinition), Shape: observedShape, Validate: ValidateObserved, Canonical: canonicalObserved}.Codec(),
	}
}

func desiredShape(data json.RawMessage) error {
	return modelShape(data, []string{"privileges", "revoked", "comment"})
}

func observedShape(data json.RawMessage) error {
	return modelShape(data, []string{"privileges", "revoked"})
}

func modelShape(data json.RawMessage, fields []string) error {
	values, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "default privilege", Allowed: fields, NonEmpty: fields})
	if err != nil {
		return err
	}
	raw, present := values["privileges"]
	if !present {
		return nil
	}
	return grantListShape(raw)
}

func grantListShape(raw json.RawMessage) error {
	var grants []json.RawMessage
	if err := json.Unmarshal(raw, &grants); err != nil {
		return fmt.Errorf("%w: decode default privilege grants: %v", schemaext.ErrInvalidValue, err)
	}
	for _, grant := range grants {
		_, err := schemaext.DecodeObject(grant, schemaext.ObjectShape{Name: "default privilege grant", Allowed: []string{"privilege", "with_option"},
			Required: []string{"privilege"}, NonEmpty: []string{"with_option"}})
		if err != nil {
			return err
		}
	}
	return nil
}

func canonicalDesired(value *Desired) *Desired {
	canonical := value.Copy()
	canonical.Privileges = canonicalGrants(value.Privileges)
	canonical.Revoked = canonicalNames(value.Revoked)
	return canonical
}

func canonicalObserved(value *Observed) *Observed {
	canonical := value.Copy()
	canonical.Privileges = canonicalGrants(value.Privileges)
	canonical.Revoked = canonicalNames(value.Revoked)
	return canonical
}
