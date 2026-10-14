package pgdefaultprivilege

import (
	"encoding/json"

	"ptah.run/core/schemaext"
)

const changeDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
	`"before":` + observedDefinition + `,"restore":` + observedDefinition + `,` +
	`"granted":{"type":"array","minItems":1,"items":` + grantDefinition + `},` +
	`"revoked":{"type":"array","minItems":1,"items":` + grantDefinition + `},` +
	`"grant_options_added":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},` +
	`"grant_options_revoked":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},` +
	`"unknown_before":{"const":true}}}`

// ChangeCodec encodes a captured delta or an exact restoration. Nested models
// use the same strict wire as standalone observations. Lists are canonical sets;
// operands are independent copies, and unknown initial state stays explicit.
func ChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*Change]{Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(changeDefinition), Shape: changeShape,
		Validate: (*Change).Validate, Canonical: canonicalChange, Clone: (*Change).Copy}.Codec()
}

func changeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "default privilege change",
		Allowed:  []string{"before", "restore", "granted", "revoked", "grant_options_added", "grant_options_revoked", "unknown_before"},
		NonEmpty: []string{"granted", "revoked", "grant_options_added", "grant_options_revoked", "unknown_before"}})
	if err != nil {
		return err
	}
	for _, key := range []string{"before", "restore"} {
		if raw, present := fields[key]; present {
			if err := observedShape(raw); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"granted", "revoked"} {
		if raw, present := fields[key]; present {
			if err := grantListShape(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func canonicalChange(v *Change) *Change {
	result := v.Copy()
	result.Before = v.Before.Canonical()
	result.Restore = v.Restore.Canonical()
	result.Granted = canonicalGrants(v.Granted)
	result.Revoked = canonicalGrants(v.Revoked)
	result.GrantOptionsAdded = canonicalNames(v.GrantOptionsAdded)
	result.GrantOptionsRevoked = canonicalNames(v.GrantOptionsRevoked)
	return result
}
