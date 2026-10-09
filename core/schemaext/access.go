package schemaext

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
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

// Validate refuses an unrecognized or missing assessment and a reason that is
// empty, padded with spaces, or contains control characters.
func (e AccessEffect) Validate() error {
	if !e.Access.Valid() {
		return fmt.Errorf("%w: unrecognized access assessment %q", ErrInvalidValue, e.Access)
	}
	if e.Reason == "" || strings.TrimSpace(e.Reason) != e.Reason || strings.ContainsFunc(e.Reason, func(r rune) bool {
		return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
	}) {
		return fmt.Errorf("%w: access assessment %q needs a reason on one trimmed line", ErrInvalidValue, e.Access)
	}
	return nil
}

type accessEffectRecord struct {
	Access Access `json:"access"`
	Reason string `json:"reason"`
}

// MarshalJSON writes the explicit record after validating it.
func (e AccessEffect) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(accessEffectRecord(e))
}

// UnmarshalJSON reads the explicit record. Unknown or duplicate fields, a
// missing field, and an invalid assessment are refused; the receiver is left
// unchanged on error.
func (e *AccessEffect) UnmarshalJSON(data []byte) error {
	record, err := DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return err
	}
	if _, found := record["access"]; !found {
		return fmt.Errorf("%w: access assessment has no access field", ErrInvalidValue)
	}
	if _, found := record["reason"]; !found {
		return fmt.Errorf("%w: access assessment has no reason field", ErrInvalidValue)
	}
	decoded, err := DecodeJSON[accessEffectRecord](data)
	if err != nil {
		return err
	}
	effect := AccessEffect(decoded)
	if err := effect.Validate(); err != nil {
		return err
	}
	*e = effect
	return nil
}

// AccessEffectSchema returns the JSON Schema of the encoded record. An owner
// embeds it in the Definition of a change or operation codec that carries an
// assessment, so the definition hash changes with the record's shape.
func AccessEffectSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","required":["access","reason"],"additionalProperties":false,` +
		`"properties":{"access":{"enum":["widens","narrows","unchanged","unknown"]},"reason":{"type":"string","minLength":1}}}`)
}

// AccessEffectSource provides an owner's access assessment for a change or
// operation payload. Like EffectSource, it must be pure and local: the owner
// computes the assessment from the model, enforcement state, and sibling
// objects it captured, and the payload carries the result as data. A payload
// that implements this interface must return a valid assessment: ValidatePayload
// refuses it otherwise, so a codec that drops the record fails when it decodes.
// A payload without this interface makes no claim about access.
type AccessEffectSource interface {
	AccessEffect() AccessEffect
}
