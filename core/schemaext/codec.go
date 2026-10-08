package schemaext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

var (
	// ErrInvalidCodec identifies incomplete or conflicting codec registration.
	ErrInvalidCodec = errors.New("invalid feature codec registration")
	// ErrUnknownCodec refuses a kind/representation the selected runtime lacks.
	ErrUnknownCodec = errors.New("unknown feature codec")
	// ErrIncompatibleCodec refuses a version, owner, or definition mismatch.
	ErrIncompatibleCodec = errors.New("incompatible feature codec")
)

// Representation distinguishes meanings that may share a semantic kind but
// have different concrete types and codecs. It is separate from target support.
type Representation string

const (
	// Desired encodes author intent, including declared defaults and omissions.
	Desired Representation = "desired"
	// Observed encodes inspection results, without claiming desired intent.
	Observed Representation = "observed"
	// Operation encodes an owner-defined AST operation payload.
	Operation Representation = "operation"
	// Change encodes a self-contained directional change payload.
	Change Representation = "change"
)

func (r Representation) valid() bool {
	return r == Desired || r == Observed || r == Operation || r == Change
}

// Codec declares a local, versioned payload encoding. Definition is the full
// owner-supplied wire-model description; its canonical hash enters envelopes and
// fingerprints. A changed definition cannot reuse an old recorded approval.
//
// Clone, Encode, Decode, and Canonical must be pure local functions. They are not
// provider transport callbacks. Registry exposes context-aware batch methods;
// an external adapter serializes data already received by a coarse service call.
// Canonical defines feature-specific set ordering while retaining ordered lists.
// It is not used as a substitute for semantic equality or target comparison.
type Codec struct {
	Prototype      Payload
	Representation Representation
	Version        uint32
	Definition     json.RawMessage
	Clone          func(Payload) (Payload, error)
	Encode         func(Payload) (json.RawMessage, error)
	Decode         func(json.RawMessage) (Payload, error)
	Canonical      func(Payload) (json.RawMessage, error)
}

// OwnedCodec binds a codec to the explicitly selected provider that owns it.
type OwnedCodec struct {
	Owner string
	Codec Codec
}

type codecKey struct {
	kind           Kind
	representation Representation
}

type registeredCodec struct {
	owner      string
	version    uint32
	definition string
	typeOf     reflect.Type
	clone      func(Payload) (Payload, error)
	encode     func(Payload) (json.RawMessage, error)
	decode     func(json.RawMessage) (Payload, error)
	canonical  func(Payload) (json.RawMessage, error)
}

// Registry is a frozen set of model codecs, independent of target handlers and
// server capabilities. Its zero value knows no kinds. There is one current
// codec per kind/representation; older encodings are refused, never guessed.
type Registry struct {
	codecs map[codecKey]registeredCodec
}

// NewRegistry validates registrations and freezes their metadata. It calls no
// encoder or decoder and performs no database or network I/O.
func NewRegistry(owned ...OwnedCodec) (Registry, error) {
	result := Registry{codecs: make(map[codecKey]registeredCodec, len(owned))}
	for _, declaration := range owned {
		key, codec, err := registerCodec(declaration)
		if err != nil {
			return Registry{}, err
		}
		if previous, exists := result.codecs[key]; exists {
			return Registry{}, fmt.Errorf("%w: %q/%s owned by both %q and %q",
				ErrInvalidCodec, key.kind, key.representation, previous.owner, codec.owner)
		}
		result.codecs[key] = codec
	}
	return result, nil
}

func registerCodec(owned OwnedCodec) (codecKey, registeredCodec, error) {
	codec := owned.Codec
	if !Kind(owned.Owner).Valid() || !codec.Representation.valid() || codec.Version == 0 {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: invalid owner, representation, or version", ErrInvalidCodec)
	}
	if err := ValidatePayload(codec.Prototype); err != nil {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: %w", ErrInvalidCodec, err)
	}
	if codec.Clone == nil || codec.Encode == nil || codec.Decode == nil || codec.Canonical == nil {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: %q is incomplete", ErrInvalidCodec, codec.Prototype.Kind())
	}
	definition, err := CanonicalJSON(codec.Definition)
	if err != nil {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: definition: %w", ErrInvalidCodec, err)
	}
	if len(definition) == 0 || definition[0] != '{' || string(definition) == "{}" {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: definition must be a nonempty object", ErrInvalidCodec)
	}
	return codecKey{kind: codec.Prototype.Kind(), representation: codec.Representation}, registeredCodec{
		owner: owned.Owner, version: codec.Version, definition: digest(definition), typeOf: reflect.TypeOf(codec.Prototype),
		clone: codec.Clone, encode: codec.Encode, decode: codec.Decode, canonical: codec.Canonical,
	}, nil
}

// Envelope is the explicit wire boundary for a concrete payload. Identity never
// includes a Go package or type name. Owner and definition bind the bytes to the
// selected interpretation; none of these fields may be omitted during decoding.
type Envelope struct {
	Format         uint32          `json:"format"`
	Owner          string          `json:"owner"`
	Kind           Kind            `json:"kind"`
	Representation Representation  `json:"representation"`
	Version        uint32          `json:"version"`
	Definition     string          `json:"definition"`
	Payload        json.RawMessage `json:"payload"`
}

const envelopeFormat uint32 = 1

// CodecIdentity identifies a selected interpretation without exposing mutable
// registration metadata or implying that a source inspected this model.
type CodecIdentity struct {
	Owner          string         `json:"owner"`
	Kind           Kind           `json:"kind"`
	Representation Representation `json:"representation"`
	Version        uint32         `json:"version"`
	Definition     string         `json:"definition"`
}

// Definitions returns selected model identities in kind/representation order.
// A reader must explicitly account for the kinds it describes; registering one
// does not retroactively make an older document authoritative for its absence.
func (r Registry) Definitions() []CodecIdentity {
	result := make([]CodecIdentity, 0, len(r.codecs))
	for key, codec := range r.codecs {
		result = append(result, CodecIdentity{Owner: codec.owner, Kind: key.kind,
			Representation: key.representation, Version: codec.version, Definition: codec.definition})
	}
	slices.SortFunc(result, func(a, b CodecIdentity) int {
		if a.Kind != b.Kind {
			return strings.Compare(string(a.Kind), string(b.Kind))
		}
		return strings.Compare(string(a.Representation), string(b.Representation))
	})
	return result
}

// Encode serializes an ordered batch through its registered local codecs.
// Any error or cancellation discards the whole result. Input payloads are
// cloned before owner code sees them; returned bytes are independently owned.
func (r Registry) Encode(ctx context.Context, representation Representation, payloads []Payload) ([]Envelope, error) {
	return r.encode(ctx, representation, payloads, func(codec registeredCodec, value Payload) (json.RawMessage, error) {
		return codec.encode(value)
	})
}

func (r Registry) encode(ctx context.Context, representation Representation, payloads []Payload, encode func(registeredCodec, Payload) (json.RawMessage, error)) ([]Envelope, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	if !representation.valid() {
		return nil, fmt.Errorf("%w: invalid representation %q", ErrInvalidValue, representation)
	}
	result := make([]Envelope, 0, len(payloads))
	for _, payload := range payloads {
		if err := codecContext(ctx); err != nil {
			return nil, err
		}
		encoded, err := r.encodeOne(representation, payload, encode)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r Registry) encodeOne(representation Representation, payload Payload, encode func(registeredCodec, Payload) (json.RawMessage, error)) (Envelope, error) {
	if err := ValidatePayload(payload); err != nil {
		return Envelope{}, err
	}
	kind := payload.Kind()
	codec, found := r.codecs[codecKey{kind: kind, representation: representation}]
	if !found {
		return Envelope{}, &UnknownCodecError{Kind: kind, Representation: representation}
	}
	cloned, err := codec.snapshot(payload)
	if err != nil {
		return Envelope{}, err
	}
	data, err := encode(codec, cloned)
	if err != nil {
		return Envelope{}, err
	}
	if err := samePayload(kind, codec.typeOf, cloned); err != nil {
		return Envelope{}, err
	}
	data, err = CanonicalJSON(data)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Format: envelopeFormat, Owner: codec.owner, Kind: kind, Representation: representation,
		Version: codec.version, Definition: codec.definition, Payload: data}, nil
}

func (c registeredCodec) snapshot(payload Payload) (Payload, error) {
	if reflect.TypeOf(payload) != c.typeOf {
		return nil, fmt.Errorf("%w: %q has an unregistered concrete type %T", ErrInvalidValue, payload.Kind(), payload)
	}
	kind := payload.Kind()
	cloned, err := c.clone(payload)
	if err != nil {
		return nil, err
	}
	if err := samePayload(kind, c.typeOf, cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// Decode reconstructs an ordered batch using exactly the selected definitions.
// It refuses unknown, incompatible, and malformed envelopes without returning
// partial values. Decoders never receive an aliased caller buffer.
func (r Registry) Decode(ctx context.Context, envelopes []Envelope) ([]Payload, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	result := make([]Payload, 0, len(envelopes))
	for _, envelope := range envelopes {
		if err := codecContext(ctx); err != nil {
			return nil, err
		}
		value, err := r.decodeOne(envelope)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r Registry) decodeOne(envelope Envelope) (Payload, error) {
	codec, found := r.codecs[codecKey{kind: envelope.Kind, representation: envelope.Representation}]
	if !found {
		return nil, &UnknownCodecError{Kind: envelope.Kind, Representation: envelope.Representation}
	}
	if envelope.Format != envelopeFormat || envelope.Owner != codec.owner || envelope.Version != codec.version || envelope.Definition != codec.definition {
		return nil, fmt.Errorf("%w: %q/%s; decode with the recorded definition or regenerate the artifact",
			ErrIncompatibleCodec, envelope.Kind, envelope.Representation)
	}
	data, err := CanonicalJSON(envelope.Payload)
	if err != nil {
		return nil, err
	}
	decoded, err := codec.decode(slices.Clone(data))
	if err != nil {
		return nil, err
	}
	if err := ValidatePayload(decoded); err != nil {
		return nil, err
	}
	if decoded.Kind() != envelope.Kind {
		return nil, fmt.Errorf("%w: decoder returned kind %q for %q", ErrInvalidValue, decoded.Kind(), envelope.Kind)
	}
	return codec.snapshot(decoded)
}

// Fingerprint hashes a canonical ordered batch, including owner, representation,
// version, and definition identity. It is a durable artifact identity, never a
// claim that desired and observed values have equivalent database semantics.
func (r Registry) Fingerprint(ctx context.Context, representation Representation, payloads []Payload) (string, error) {
	envelopes, err := r.encode(ctx, representation, payloads, func(codec registeredCodec, value Payload) (json.RawMessage, error) {
		return codec.canonical(value)
	})
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(wireBatch{Format: envelopeFormat, Values: envelopes})
	if err != nil {
		return "", err
	}
	return digest(data), nil
}

type wireBatch struct {
	Format uint32     `json:"format"`
	Values []Envelope `json:"values"`
}

// Marshal writes a versioned payload document, including a format for an empty
// batch. It uses only explicitly selected concrete codecs.
func (r Registry) Marshal(ctx context.Context, representation Representation, payloads []Payload) ([]byte, error) {
	envelopes, err := r.Encode(ctx, representation, payloads)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wireBatch{Format: envelopeFormat, Values: envelopes})
}

// Unmarshal refuses unknown envelope fields, duplicate keys, unsupported
// document formats, and incomplete documents before reconstructing payloads.
func (r Registry) Unmarshal(ctx context.Context, data []byte) ([]Payload, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	document, err := DecodeJSON[wireBatch](data)
	if err != nil {
		return nil, err
	}
	if document.Format != envelopeFormat || document.Values == nil {
		return nil, fmt.Errorf("%w: invalid payload document format or missing values", ErrIncompatibleCodec)
	}
	return r.Decode(ctx, document.Values)
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func codecContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: codec operation requires a context", ErrInvalidValue)
	}
	return ctx.Err()
}
