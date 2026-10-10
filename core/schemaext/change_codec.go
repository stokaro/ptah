package schemaext

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
)

// EncodedChange is the explicit wire form of a [ChangeRecord]: the structured
// subject beside an envelope that names the owner, the namespaced change kind,
// the change representation and the codec version that wrote the payload.
type EncodedChange struct {
	Subject objectidentity.ID `json:"subject"`
	Value   Envelope          `json:"value"`
}

// EncodeChanges serializes ordered change records through their registered
// change codecs. A record whose kind has no change codec in the registry
// returns an [UnknownCodecError] naming the kind. Invalid subjects or payloads
// wrap [ErrInvalidValue]. Any error or cancellation discards the whole batch;
// the input records are not modified.
func (r Registry) EncodeChanges(ctx context.Context, changes []ChangeRecord) ([]EncodedChange, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	payloads := make([]Payload, len(changes))
	for i, change := range changes {
		if err := validChangeSubject(change.Subject); err != nil {
			return nil, err
		}
		payloads[i] = change.Value
	}
	// Encode validates each payload before it encodes it.
	envelopes, err := r.Encode(ctx, Change, payloads)
	if err != nil {
		return nil, err
	}
	result := make([]EncodedChange, len(changes))
	for i, change := range changes {
		result[i] = EncodedChange{Subject: change.Subject, Value: envelopes[i]}
	}
	return result, nil
}

// DecodeChanges reconstructs ordered change records with exactly the selected
// change codecs. An envelope recorded under another representation, or a
// payload that does not decode to a change value, wraps [ErrInvalidValue]; an
// unknown kind returns an [UnknownCodecError] and a different owner, version
// or definition wraps [ErrIncompatibleCodec]. No partial batch is returned.
func (r Registry) DecodeChanges(ctx context.Context, encoded []EncodedChange) ([]ChangeRecord, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	envelopes := make([]Envelope, len(encoded))
	for i, change := range encoded {
		if err := validChangeSubject(change.Subject); err != nil {
			return nil, err
		}
		if change.Value.Representation != Change {
			return nil, fmt.Errorf("%w: change %q is recorded as %q", ErrInvalidValue, change.Value.Kind, change.Value.Representation)
		}
		envelopes[i] = change.Value
	}
	payloads, err := r.Decode(ctx, envelopes)
	if err != nil {
		return nil, err
	}
	result := make([]ChangeRecord, len(encoded))
	for i, payload := range payloads {
		value, ok := payload.(ChangeValue)
		if !ok {
			return nil, fmt.Errorf("%w: %q does not decode to a change", ErrInvalidValue, payload.Kind())
		}
		result[i] = ChangeRecord{Subject: encoded[i].Subject, Value: value}
	}
	return result, nil
}

func validChangeSubject(subject objectidentity.ID) error {
	if subject.Kind == "" || subject.Name.Source == "" || subject.Name.Normalized == "" {
		return fmt.Errorf("%w: a change requires a structured subject", ErrInvalidValue)
	}
	return nil
}
