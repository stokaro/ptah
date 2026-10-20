package providerhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/schemaext"
)

type modelKey struct {
	kind           schemaext.Kind
	representation schemaext.Representation
}

type modelContract struct {
	key        modelKey
	engines    map[string]*protocol.ValueModel
	definition json.RawMessage
	signature  string
}

// resolvedModel contains only values validated by an explicit owner batch. It
// retains no process client; copying or comparing it cannot perform an RPC.
type resolvedModel struct {
	kind      schemaext.Kind
	signature string
	wire      modelWire
	canonical json.RawMessage
}

func (v *resolvedModel) Kind() schemaext.Kind { return v.kind }
func (v *resolvedModel) Clone() schemaext.Value {
	return &resolvedModel{kind: v.kind, signature: v.signature, wire: v.wire.clone(), canonical: bytes.Clone(v.canonical)}
}
func (v *resolvedModel) Equal(other schemaext.Value) bool {
	right, ok := other.(*resolvedModel)
	return ok && right != nil && v.kind == right.kind && v.signature == right.signature && bytes.Equal(v.canonical, right.canonical)
}

func (h *Host) initializeModels() error {
	h.models = make(map[modelKey]*modelContract)
	for _, engine := range h.description.GetEngines() {
		for _, model := range engine.GetValueModels() {
			representation, err := modelRepresentation(model.GetRepresentation())
			if err != nil {
				return err
			}
			key := modelKey{kind: schemaext.Kind(model.GetKind()), representation: representation}
			if !key.kind.Valid() {
				return fmt.Errorf("%w: provider model kind is not namespaced", schemaext.ErrInvalidCodec)
			}
			contract := h.models[key]
			if contract == nil {
				contract = &modelContract{key: key, engines: make(map[string]*protocol.ValueModel)}
				h.models[key] = contract
			}
			contract.engines[engine.GetId()] = model
		}
	}
	if len(h.models) > 0 && !slices.Contains(h.description.GetFeatures(), provider.NormalizeValuesFeature) {
		return fmt.Errorf("%w: declared models require owner normalization", provider.ErrProtocol)
	}
	keys := make([]modelKey, 0, len(h.models))
	for key := range h.models {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b modelKey) int {
		if a.kind != b.kind {
			return strings.Compare(string(a.kind), string(b.kind))
		}
		return strings.Compare(string(a.representation), string(b.representation))
	})
	for _, key := range keys {
		contract := h.models[key]
		definition, err := h.modelDefinition(contract)
		if err != nil {
			return err
		}
		contract.definition = definition
		digest := sha256.Sum256(definition)
		contract.signature = "sha256:" + hex.EncodeToString(digest[:])
		h.modelCodecs = append(h.modelCodecs, contract.codec())
	}
	return nil
}

func (m *modelContract) codec() schemaext.Codec {
	return schemaext.Codec{Prototype: &resolvedModel{kind: m.key.kind, signature: m.signature}, Representation: m.key.representation, Version: 1, Definition: bytes.Clone(m.definition),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := m.value(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Encode: func(payload schemaext.Payload) (json.RawMessage, error) {
			value, err := m.value(payload)
			if err != nil {
				return nil, err
			}
			return json.Marshal(value.wire)
		},
		Canonical: func(payload schemaext.Payload) (json.RawMessage, error) {
			value, err := m.value(payload)
			if err != nil {
				return nil, err
			}
			return bytes.Clone(value.canonical), nil
		},
	}
}

func (m *modelContract) value(payload schemaext.Payload) (*resolvedModel, error) {
	value, ok := payload.(*resolvedModel)
	if !ok || value == nil || value.kind != m.key.kind || value.signature != m.signature || value.wire.Format != 1 || len(value.canonical) == 0 {
		return nil, fmt.Errorf("%w: model value is unresolved or belongs to another contract", schemaext.ErrInvalidValue)
	}
	return value, nil
}

func (m *modelContract) resolved(wire modelWire, normalized *protocol.NormalizedValue) (schemaext.Payload, error) {
	wire.Subject = referenceFromProtocol(normalized.GetValue().GetOwner())
	wire.Payload = bytes.Clone(normalized.GetValue().GetPayload())
	wire.RequiredFeatures = slices.Clone(normalized.GetValue().GetRequiredFeatures())
	subject := referenceFromProtocol(normalized.GetNormalizedOwner())
	subject.DisplayName = ""
	projection := struct {
		Contract         string          `json:"contract"`
		Engine           string          `json:"engine"`
		Features         []string        `json:"features"`
		RequiredFeatures []string        `json:"required_features"`
		Subject          *modelReference `json:"subject"`
		Format           string          `json:"format"`
		Projection       []byte          `json:"projection"`
	}{m.signature, wire.Engine, featureSet(wire.Features), featureSet(wire.RequiredFeatures), subject, normalized.GetCanonicalFormat(), normalized.GetCanonicalProjection()}
	data, err := json.Marshal(projection)
	if err != nil {
		return nil, err
	}
	canonical, err := schemaext.CanonicalJSON(data)
	if err != nil {
		return nil, err
	}
	return &resolvedModel{kind: m.key.kind, signature: m.signature, wire: wire.clone(), canonical: canonical}, nil
}
