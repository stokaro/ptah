package schemaext_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

const grantKind schemaext.Kind = "example.org/access/grant-change"

// grantChange is a change payload that carries its owner's access assessment
// as data, the shape an owner uses when the assessment depends on context the
// payload alone does not hold.
type grantChange struct {
	Role   string                 `json:"role"`
	Access schemaext.AccessEffect `json:"access"`
}

func (*grantChange) Kind() schemaext.Kind { return grantKind }
func (v *grantChange) CloneChange() schemaext.ChangeValue {
	cloned := *v
	return &cloned
}
func (v *grantChange) AccessEffect() schemaext.AccessEffect { return v.Access }

func grantCodec(encode func(schemaext.Payload) (json.RawMessage, error)) schemaext.Codec {
	return schemaext.Codec{
		Prototype:      &grantChange{Access: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "prototype"}},
		Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["role","access"],"additionalProperties":false,` +
			`"properties":{"role":{"type":"string"},"access":` + string(schemaext.AccessEffectSchema()) + `}}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*grantChange).CloneChange(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			return schemaext.DecodeJSON[*grantChange](data)
		},
	}
}

func encodeGrant(payload schemaext.Payload) (json.RawMessage, error) {
	return json.Marshal(payload.(*grantChange))
}

// encodeGrantWithoutAccess is a defective codec that forgets the assessment.
func encodeGrantWithoutAccess(payload schemaext.Payload) (json.RawMessage, error) {
	return json.Marshal(struct {
		Role string `json:"role"`
	}{payload.(*grantChange).Role})
}

func grantSubject() objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(grantKind), Name: objectidentity.Part{Source: "reader", Normalized: "reader"}}
}

func TestAccessEffect_Validate_HappyPath(t *testing.T) {
	for _, access := range []schemaext.Access{schemaext.AccessWidens, schemaext.AccessNarrows, schemaext.AccessUnchanged, schemaext.AccessUnknown} {
		t.Run(string(access), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(access.Valid(), qt.IsTrue)
			c.Assert(schemaext.AccessEffect{Access: access, Reason: "the owner explains it"}.Validate(), qt.IsNil)
		})
	}
}

func TestAccessEffect_Validate_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		effect schemaext.AccessEffect
	}{
		{name: "zero value", effect: schemaext.AccessEffect{}},
		{name: "missing assessment", effect: schemaext.AccessEffect{Reason: "reason without an assessment"}},
		{name: "unrecognized assessment", effect: schemaext.AccessEffect{Access: "safe", Reason: "a spelling no owner may use"}},
		{name: "case variant", effect: schemaext.AccessEffect{Access: "Widens", Reason: "spellings are case-sensitive"}},
		{name: "missing reason", effect: schemaext.AccessEffect{Access: schemaext.AccessUnknown}},
		{name: "padded reason", effect: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: " grants reads"}},
		{name: "two-line reason", effect: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "first\nsecond"}},
		{name: "line separator", effect: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "first\u2028second"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tc.effect.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestAccessEffect_JSON_HappyPath(t *testing.T) {
	c := qt.New(t)
	effect := schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy admits more rows"}
	data, err := json.Marshal(effect)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"access":"widens","reason":"a permissive policy admits more rows"}`)
	var decoded schemaext.AccessEffect
	c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)
	c.Assert(decoded, qt.Equals, effect)
}

func TestAccessEffect_JSON_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, err := json.Marshal(schemaext.AccessEffect{Access: schemaext.AccessWidens})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)

	tests := []struct {
		name string
		data string
	}{
		{name: "unknown field", data: `{"access":"narrows","reason":"r","scope":"all"}`},
		{name: "missing access", data: `{"reason":"r"}`},
		{name: "missing reason", data: `{"access":"narrows"}`},
		{name: "unrecognized access", data: `{"access":"safe","reason":"r"}`},
		{name: "empty access", data: `{"access":"","reason":"r"}`},
		{name: "blank reason", data: `{"access":"unknown","reason":" "}`},
		{name: "duplicate field", data: `{"access":"narrows","access":"widens","reason":"r"}`},
		{name: "not an object", data: `"widens"`},
		{name: "null", data: `null`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			kept := schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "kept"}
			decoded := kept
			c.Assert(decoded.UnmarshalJSON([]byte(tc.data)), qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.Equals, kept)
		})
	}
}

// TestAccessEffectSchema_NamesExactlyTheValidAssessments ties the published
// record schema to Valid, so a fifth assessment cannot be added to one alone.
func TestAccessEffectSchema_NamesExactlyTheValidAssessments(t *testing.T) {
	c := qt.New(t)
	schema, err := schemaext.DecodeJSON[struct {
		Type                 string   `json:"type"`
		Required             []string `json:"required"`
		AdditionalProperties bool     `json:"additionalProperties"`
		Properties           struct {
			Access struct {
				Enum []schemaext.Access `json:"enum"`
			} `json:"access"`
			Reason struct {
				Type      string `json:"type"`
				MinLength int    `json:"minLength"`
			} `json:"reason"`
		} `json:"properties"`
	}](schemaext.AccessEffectSchema())
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Required, qt.DeepEquals, []string{"access", "reason"})
	c.Assert(schema.AdditionalProperties, qt.IsFalse)
	c.Assert(schema.Properties.Access.Enum, qt.DeepEquals,
		[]schemaext.Access{schemaext.AccessWidens, schemaext.AccessNarrows, schemaext.AccessUnchanged, schemaext.AccessUnknown})
	for _, access := range schema.Properties.Access.Enum {
		c.Assert(access.Valid(), qt.IsTrue, qt.Commentf("%s", access))
	}
	c.Assert(schema.Properties.Reason.MinLength, qt.Equals, 1)
}

func TestValidatePayload_HappyPath_AcceptsAnAssessedPayload(t *testing.T) {
	c := qt.New(t)
	payload := &grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "revokes reads"}}
	c.Assert(schemaext.ValidatePayload(payload), qt.IsNil)
	cloned, err := schemaext.ChangeRecord{Subject: grantSubject(), Value: payload}.Clone()
	c.Assert(err, qt.IsNil)
	c.Assert(cloned.Value, qt.DeepEquals, schemaext.ChangeValue(payload))
}

func TestValidatePayload_FailurePath_RefusesAnUnassessedPayload(t *testing.T) {
	c := qt.New(t)
	payload := &grantChange{Role: "reader"}
	c.Assert(schemaext.ValidatePayload(payload), qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err := schemaext.ChangeRecord{Subject: grantSubject(), Value: payload}.Clone()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

func TestRegistry_CarriesAccessEffects_HappyPath(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(grantCodec(encodeGrant)))
	c.Assert(err, qt.IsNil)
	payload := &grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "grants reads"}}
	data, err := registry.Marshal(context.Background(), schemaext.Change, []schemaext.Payload{payload})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `"access":{"access":"widens","reason":"grants reads"}`)
	decoded, err := registry.Unmarshal(context.Background(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{payload})

	snapshot, err := registry.SnapshotChanges(context.Background(), []schemaext.ChangeRecord{{Subject: grantSubject(), Value: payload}})
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot[0].Value.(schemaext.AccessEffectSource).AccessEffect(), qt.Equals, payload.Access)

	widened, err := registry.Fingerprint(context.Background(), schemaext.Change, []schemaext.Payload{payload})
	c.Assert(err, qt.IsNil)
	narrowed, err := registry.Fingerprint(context.Background(), schemaext.Change, []schemaext.Payload{
		&grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "grants reads"}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(widened, qt.Not(qt.Equals), narrowed)
}

func TestRegistry_CarriesAccessEffects_FailurePath(t *testing.T) {
	c := qt.New(t)
	lossy, err := schemaext.NewRegistry(owned(grantCodec(encodeGrantWithoutAccess)))
	c.Assert(err, qt.IsNil)
	payload := &grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "grants reads"}}
	data, err := lossy.Marshal(context.Background(), schemaext.Change, []schemaext.Payload{payload})
	c.Assert(err, qt.IsNil)
	decoded, err := lossy.Unmarshal(context.Background(), data)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(decoded, qt.IsNil)

	registry, err := schemaext.NewRegistry(owned(grantCodec(encodeGrant)))
	c.Assert(err, qt.IsNil)
	_, err = registry.Marshal(context.Background(), schemaext.Change, []schemaext.Payload{&grantChange{Role: "reader"}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
