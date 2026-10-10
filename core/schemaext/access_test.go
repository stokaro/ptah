package schemaext_test

import (
	"context"
	"encoding/json"
	"regexp"
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
		Prototype: &grantChange{}, Representation: schemaext.Change, Version: 1,
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
		{name: "invalid UTF-8", effect: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "grants \xff reads"}},
		{name: "no-break space padding", effect: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "grants reads\u00a0"}},
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

// A record with several unknown fields is refused with one message, naming
// the first in sorted order, however the decoder happened to read them.
func TestAccessEffect_JSON_FailurePath_NamesTheSameUnknownFieldEveryTime(t *testing.T) {
	c := qt.New(t)
	data := []byte(`{"access":"narrows","reason":"r","zone":1,"scope":"all","mode":2,"area":3}`)
	for range 50 {
		var decoded schemaext.AccessEffect
		err := decoded.UnmarshalJSON(data)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(err, qt.ErrorMatches, `.*access assessment has an unknown field "area"`)
	}
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
		{name: "case variant key", data: `{"ACCESS":"widens","access":"narrows","reason":"r"}`},
		{name: "case variant key alone", data: `{"Access":"narrows","reason":"r"}`},
		{name: "access is not a string", data: `{"access":1,"reason":"r"}`},
		{name: "reason is not a string", data: `{"access":"narrows","reason":["r"]}`},
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

// TestAccessEffectSchema_MatchesValidate ties the published record schema to
// Validate, so neither can admit an assessment or a reason the other refuses.
func TestAccessEffectSchema_MatchesValidate(t *testing.T) {
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
				Pattern   string `json:"pattern"`
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
	pattern, err := regexp.Compile(schema.Properties.Reason.Pattern)
	c.Assert(err, qt.IsNil)
	samples := []string{
		"grants reads", "a", "caf\u00e9 policy", "two  spaces inside", "trailing dot.",
		"", " padded", "padded ", "\tpadded", "padded\u00a0", "\u3000padded", "padded\u2000", "\u0085padded",
		"two\nlines", "tab\tinside", "line\u2028separator", "paragraph\u2029separator", "delete\u007fcharacter",
		"next\u0085line", "escape\u001bcharacter", "a\u00a0b", "a\u3000b",
	}
	for _, sample := range samples {
		valid := schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: sample}.Validate() == nil
		c.Assert(pattern.MatchString(sample), qt.Equals, valid, qt.Commentf("%q", sample))
	}
}

func TestChangeRecordClone_HappyPath_KeepsTheAccessAssessment(t *testing.T) {
	c := qt.New(t)
	payload := &grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "revokes reads"}}
	cloned, err := schemaext.ChangeRecord{Subject: grantSubject(), Value: payload}.Clone()
	c.Assert(err, qt.IsNil)
	c.Assert(cloned.Value, qt.DeepEquals, schemaext.ChangeValue(payload))
}

// ValidatePayload checks identity only, so a codec prototype and a lint pass
// need no assessment; the clone and every codec boundary refuse its absence.
func TestChangeRecordClone_FailurePath_RefusesAnUnassessedPayload(t *testing.T) {
	c := qt.New(t)
	payload := &grantChange{Role: "reader"}
	c.Assert(schemaext.ValidatePayload(payload), qt.IsNil)
	cloned, err := schemaext.ChangeRecord{Subject: grantSubject(), Value: payload}.Clone()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(cloned, qt.DeepEquals, schemaext.ChangeRecord{})
}

// droppingGrant is a change whose clone forgets the assessment.
type droppingGrant struct{ grantChange }

func (v *droppingGrant) CloneChange() schemaext.ChangeValue {
	return &droppingGrant{grantChange{Role: v.Role}}
}

func TestChangeRecordClone_FailurePath_RefusesACloneThatDropsTheAssessment(t *testing.T) {
	c := qt.New(t)
	payload := &droppingGrant{grantChange{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "grants reads"}}}
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
