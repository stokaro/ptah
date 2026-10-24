package schemaext

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
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
// Clone, Encode, Decode, and Canonical are pure local functions, never
// provider transport callbacks. OwnedCodec.BatchDecoder replaces Decode at the
// explicit batch boundary when models need their external owner. Registry
// exposes context-aware batch methods; an external adapter serializes data
// already received by a coarse service call.
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
	// BatchDecoder resolves all this owner's envelopes in one context-aware
	// call. It is mutually exclusive with Codec.Decode. Reuse the same pointer
	// for every registration belonging to one owner; registration snapshots it.
	BatchDecoder *BatchDecoder
}

// BatchDecoder validates serialized models through their selected owner. It
// receives a complete ordered group of envelopes that may contain several
// kinds and representations. Decode must return one payload per envelope in
// the same order, or an error and no usable prefix. It must honor cancellation.
// This is an explicit resolution phase; Clone, Equal, Encode, and Canonical
// remain local operations and never call this service.
type BatchDecoder struct {
	Decode func(context.Context, []Envelope) ([]Payload, error)
}

type codecKey struct {
	kind           Kind
	representation Representation
}

type registeredCodec struct {
	owner      string
	version    uint32
	definition *definitionRef
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
	codecs        map[codecKey]registeredCodec
	batchDecoders map[string]func(context.Context, []Envelope) ([]Payload, error)
	// definitions lists the registered keys in Definitions' order, sorted once
	// when the registry is frozen. The identities, which carry the digests, are
	// built when a caller asks for them.
	definitions []codecKey
}

// NewRegistry validates registrations and freezes their metadata. It calls no
// encoder or decoder and performs no database or network I/O.
func NewRegistry(owned ...OwnedCodec) (Registry, error) {
	result := Registry{codecs: make(map[codecKey]registeredCodec, len(owned)), batchDecoders: make(map[string]func(context.Context, []Envelope) ([]Payload, error))}
	batchOwners := make(map[string]*BatchDecoder)
	for _, declaration := range owned {
		if previous, found := batchOwners[declaration.Owner]; found && previous != declaration.BatchDecoder {
			return Registry{}, fmt.Errorf("%w: owner %q has conflicting batch decoders", ErrInvalidCodec, declaration.Owner)
		}
		batchOwners[declaration.Owner] = declaration.BatchDecoder
		key, codec, err := registerCodec(declaration)
		if err != nil {
			return Registry{}, err
		}
		if previous, exists := result.codecs[key]; exists {
			return Registry{}, fmt.Errorf("%w: %q/%s owned by both %q and %q",
				ErrInvalidCodec, key.kind, key.representation, previous.owner, codec.owner)
		}
		result.codecs[key] = codec
		if declaration.BatchDecoder != nil {
			result.batchDecoders[declaration.Owner] = declaration.BatchDecoder.Decode
		}
	}
	result.definitions = make([]codecKey, 0, len(result.codecs))
	for key := range result.codecs {
		result.definitions = append(result.definitions, key)
	}
	slices.SortFunc(result.definitions, func(a, b codecKey) int {
		if a.kind != b.kind {
			return strings.Compare(string(a.kind), string(b.kind))
		}
		return strings.Compare(string(a.representation), string(b.representation))
	})
	return result, nil
}

// identity is the registered codec's public identity under key.
func (c registeredCodec) identity(key codecKey) CodecIdentity {
	return CodecIdentity{Owner: c.owner, Kind: key.kind, Representation: key.representation, Version: c.version, Definition: c.definition.digest()}
}

// Identity returns the identity of the codec registered for kind in
// representation, and false when the registry has none. It answers what
// [Registry.Definitions] lists, without copying the whole list.
func (r Registry) Identity(kind Kind, representation Representation) (CodecIdentity, bool) {
	key := codecKey{kind: kind, representation: representation}
	codec, found := r.codecs[key]
	if !found {
		return CodecIdentity{}, false
	}
	return codec.identity(key), true
}

// Owner returns the owner of the codec registered for kind in representation,
// and false when the registry has none. It answers what [Registry.Identity]
// reports as Owner, without computing the definition digest the identity
// carries.
func (r Registry) Owner(kind Kind, representation Representation) (string, bool) {
	codec, found := r.codecs[codecKey{kind: kind, representation: representation}]
	return codec.owner, found
}

func registerCodec(owned OwnedCodec) (codecKey, registeredCodec, error) {
	codec := owned.Codec
	if !Kind(owned.Owner).Valid() || !codec.Representation.valid() || codec.Version == 0 {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: invalid owner, representation, or version", ErrInvalidCodec)
	}
	if err := ValidatePayload(codec.Prototype); err != nil {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: %w", ErrInvalidCodec, err)
	}
	if codec.Clone == nil || codec.Encode == nil || codec.Canonical == nil || !validDecoder(owned) {
		return codecKey{}, registeredCodec{}, fmt.Errorf("%w: %q is incomplete", ErrInvalidCodec, codec.Prototype.Kind())
	}
	definition, err := newDefinitionRef(codec.Definition)
	if err != nil {
		return codecKey{}, registeredCodec{}, err
	}
	return codecKey{kind: codec.Prototype.Kind(), representation: codec.Representation}, registeredCodec{
		owner: owned.Owner, version: codec.Version, definition: definition, typeOf: reflect.TypeOf(codec.Prototype),
		clone: codec.Clone, encode: codec.Encode, decode: codec.Decode, canonical: codec.Canonical,
	}, nil
}

// definitionDigests memoizes [definitionRef.digest] by definition text. A codec
// definition is a constant of its owner, and a runtime is assembled from the
// same definitions many times in one process.
var definitionDigests sync.Map

// definitionRef is a codec definition that was checked when the codec was
// registered and is digested when a digest is first asked for. Registering a
// codec happens for every codec of every provider in every process, while only
// an encode, a decode or a listing needs the digest, so canonicalizing each
// definition at registration allocated megabytes no command used.
type definitionRef struct {
	text   string
	once   sync.Once
	result string
}

// newDefinitionRef refuses a definition that [CanonicalJSON] refuses or that is
// not a nonempty object, and keeps its text for [definitionRef.digest].
func newDefinitionRef(definition json.RawMessage) (*definitionRef, error) {
	if !canonicalizable(definition) {
		// CanonicalJSON repeats the check to produce the refusal text.
		if _, err := CanonicalJSON(definition); err != nil {
			return nil, fmt.Errorf("%w: definition: %w", ErrInvalidCodec, err)
		}
	}
	if !isNonemptyObject(definition) {
		return nil, fmt.Errorf("%w: definition must be a nonempty object", ErrInvalidCodec)
	}
	return &definitionRef{text: string(definition)}, nil
}

// isNonemptyObject reports whether valid JSON text is an object with at least
// one member, ignoring surrounding whitespace.
func isNonemptyObject(definition []byte) bool {
	trimmed := bytes.TrimLeft(definition, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	members := bytes.TrimLeft(trimmed[1:], " \t\r\n")
	return len(members) > 0 && members[0] != '}'
}

// digest returns the definition's digest, computing it once.
func (d *definitionRef) digest() string {
	d.once.Do(func() {
		if known, found := definitionDigests.Load(d.text); found {
			if memoized, isText := known.(string); isText {
				d.result = memoized
				return
			}
		}
		canonical, err := CanonicalJSON([]byte(d.text))
		if err != nil {
			// newDefinitionRef refused every text CanonicalJSON refuses.
			panic("schemaext: registered definition is not canonicalizable: " + err.Error())
		}
		d.result = digest(canonical)
		definitionDigests.Store(d.text, d.result)
	})
	return d.result
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
	identities := make([]CodecIdentity, len(r.definitions))
	for i, key := range r.definitions {
		identities[i] = r.codecs[key].identity(key)
	}
	return identities
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
		Version: codec.version, Definition: codec.definition.digest(), Payload: data}, nil
}

// snapshot clones a payload through its codec. Every codec boundary passes
// through here, so it is also where a declared access assessment is required.
// The clone is what the caller receives, so the clone is checked: an input
// without the assessment yields a clone without it, and so does a defective
// Clone.
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
	if err := validateAccess(cloned); err != nil {
		return nil, err
	}
	return cloned, nil
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
