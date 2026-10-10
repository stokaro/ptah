package schemaext

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Access is an owner's assessment of how a change or operation can affect what
// roles may read or write. It is separate from Impact, which describes data
// loss and object lifecycle: creating an access-control object can widen access
// and removing one can narrow it, so neither verb establishes the direction.
// The zero value records no assessment; consumers must treat it as unknown.
type Access string

const (
	// AccessWidens means the change can grant a role access it does not have
	// before the change. A change that can also restrict access reports this.
	AccessWidens Access = "widens"
	// AccessNarrows means the change can remove access a role has before the
	// change, and cannot grant access it lacks.
	AccessNarrows Access = "narrows"
	// AccessUnchanged means the owner established that no role gains or loses
	// access. Unchanged text alone does not establish logical equivalence.
	AccessUnchanged Access = "unchanged"
	// AccessUnknown means the owner cannot establish the effect, for example
	// for an arbitrary predicate change. It requires the strongest review.
	AccessUnknown Access = "unknown"
)

// Valid reports whether a is one of the four declared assessments. The zero
// value and any other spelling are invalid.
func (a Access) Valid() bool {
	switch a {
	case AccessWidens, AccessNarrows, AccessUnchanged, AccessUnknown:
		return true
	default:
		return false
	}
}

// AccessEffect is an owner's access assessment with its human explanation.
// Reason is required for every assessment, including AccessUnknown. It is not a
// serialization identity or a comparison key. The JSON form is the explicit
// record {"access": ..., "reason": ...}; encoding or decoding an invalid
// assessment fails with ErrInvalidValue, so a codec cannot write or read one.
type AccessEffect struct {
	Access Access `json:"access"`
	Reason string `json:"reason"`
}

// Validate refuses an unrecognized or missing assessment, and a reason that is
// empty, not valid UTF-8, padded with white space, or that contains a control
// character or a line or paragraph separator.
func (e AccessEffect) Validate() error {
	if !e.Access.Valid() {
		return fmt.Errorf("%w: unrecognized access assessment %q", ErrInvalidValue, e.Access)
	}
	if e.Reason == "" || !utf8.ValidString(e.Reason) || strings.TrimSpace(e.Reason) != e.Reason ||
		strings.ContainsFunc(e.Reason, forbiddenInReason) {
		return fmt.Errorf("%w: access assessment %q needs a reason on one trimmed line", ErrInvalidValue, e.Access)
	}
	return nil
}

func forbiddenInReason(r rune) bool {
	return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
}

// MarshalJSON writes the explicit record after validating it.
func (e AccessEffect) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Access Access `json:"access"`
		Reason string `json:"reason"`
	}{e.Access, e.Reason})
}

// UnmarshalJSON reads the explicit record. The object must hold exactly the
// keys "access" and "reason", spelled as written: encoding/json would match a
// key in any letter case, and the record schema admits no other property.
// Duplicate keys, values that are not strings, and an invalid assessment are
// refused; the receiver is left unchanged on error.
func (e *AccessEffect) UnmarshalJSON(data []byte) error {
	record, err := DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("%w: access assessment is not an object", ErrInvalidValue)
	}
	// Sorted, so a record with several unknown fields is refused with the
	// same message every time.
	for _, key := range slices.Sorted(maps.Keys(record)) {
		if key != "access" && key != "reason" {
			return fmt.Errorf("%w: access assessment has an unknown field %q", ErrInvalidValue, key)
		}
	}
	rawAccess, found := record["access"]
	if !found {
		return fmt.Errorf("%w: access assessment has no access field", ErrInvalidValue)
	}
	rawReason, found := record["reason"]
	if !found {
		return fmt.Errorf("%w: access assessment has no reason field", ErrInvalidValue)
	}
	access, err := DecodeJSON[string](rawAccess)
	if err != nil {
		return err
	}
	reason, err := DecodeJSON[string](rawReason)
	if err != nil {
		return err
	}
	effect := AccessEffect{Access: Access(access), Reason: reason}
	if err := effect.Validate(); err != nil {
		return err
	}
	*e = effect
	return nil
}

// accessReasonPattern is the reason constraint Validate enforces, as a JSON
// Schema pattern: no control character, line separator or paragraph separator
// anywhere, and no white space at either end. The escapes are JSON's, so the
// pattern reaches a regular expression engine as literal characters.
const accessReasonPattern = `^[^\u0000-\u0020\u007f-\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]` +
	`(?:[^\u0000-\u001f\u007f-\u009f\u2028\u2029]*[^\u0000-\u0020\u007f-\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000])?$`

// AccessEffectSchema returns the JSON Schema of the encoded record, including
// the reason constraint Validate enforces. An owner embeds it in the Definition
// of a change or operation codec that carries an assessment, so the definition
// hash changes with the record's shape.
func AccessEffectSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","required":["access","reason"],"additionalProperties":false,` +
		`"properties":{"access":{"enum":["widens","narrows","unchanged","unknown"]},` +
		`"reason":{"type":"string","minLength":1,"pattern":"` + accessReasonPattern + `"}}}`)
}

// AccessEffectSource provides an owner's access assessment for a change or
// operation payload. Like EffectSource, it must be pure and local: the owner
// computes the assessment from the model, enforcement state, and sibling
// objects it captured, and the payload carries the result as data. A payload
// that implements this interface must return a valid assessment. Codec
// snapshots, encoding, decoding, and ChangeRecord.Clone refuse it otherwise, so
// a codec that drops the record fails when it decodes. ValidatePayload checks
// identity only, so a codec prototype needs no assessment. A payload without
// this interface makes no claim about access.
type AccessEffectSource interface {
	AccessEffect() AccessEffect
}

// validateAccess refuses a payload that declares an access assessment and does
// not carry a valid one.
func validateAccess(payload Payload) error {
	source, ok := payload.(AccessEffectSource)
	if !ok {
		return nil
	}
	if err := source.AccessEffect().Validate(); err != nil {
		return fmt.Errorf("%q: %w", payload.Kind(), err)
	}
	return nil
}
