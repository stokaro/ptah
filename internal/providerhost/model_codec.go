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
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
)

type modelKey struct {
	kind           schemaext.Kind
	representation schemaext.Representation
}

type modelContract struct {
	displayName string
	key         modelKey
	engines     map[string]*protocol.ValueModel
	definition  json.RawMessage
	signature   string
}

// resolvedModel contains only values validated by an explicit owner batch. It
// retains no process client; copying or comparing it cannot perform an RPC.
type resolvedModel struct {
	kind       schemaext.Kind
	signature  string
	wire       modelWire
	canonical  json.RawMessage
	normalized *protocol.NormalizedValue
}

func (v *resolvedModel) Kind() schemaext.Kind   { return v.kind }
func (v *resolvedModel) Clone() schemaext.Value { return v.cloneModel() }
func (v *resolvedModel) cloneModel() *resolvedModel {
	return &resolvedModel{kind: v.kind, signature: v.signature, wire: v.wire.clone(), canonical: bytes.Clone(v.canonical), normalized: proto.CloneOf(v.normalized)}
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
				contract = &modelContract{key: key, displayName: model.GetDisplayName(), engines: make(map[string]*protocol.ValueModel)}
				h.models[key] = contract
			}
			if contract.displayName != model.GetDisplayName() {
				return fmt.Errorf("%w: model display name differs across engines", provider.ErrProtocol)
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
	return schemaext.Codec{Prototype: m.payload(&resolvedModel{kind: m.key.kind, signature: m.signature}), Representation: m.key.representation, Version: 1, Definition: bytes.Clone(m.definition),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := m.value(payload)
			if err != nil {
				return nil, err
			}
			return m.payload(value.cloneModel()), nil
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
	if m.key.representation == schemaext.Change {
		change, found := payload.(*resolvedChange)
		ok = found && change != nil
		if ok {
			value = change.model
		}
	}
	if m.key.representation == schemaext.Operation {
		operation, found := payload.(*resolvedOperation)
		ok = found && operation != nil
		if ok {
			value = operation.model
		}
	}
	if !ok || value == nil || value.kind != m.key.kind || value.signature != m.signature || value.wire.Format != 1 || len(value.canonical) == 0 {
		return nil, fmt.Errorf("%w: model value is unresolved or belongs to another contract", schemaext.ErrInvalidValue)
	}
	return value, nil
}

func (m *modelContract) resolved(wire modelWire, normalized *protocol.NormalizedValue) (*resolvedModel, error) {
	wire.Subject = referenceFromProtocol(normalized.GetValue().GetOwner())
	wire.Payload = bytes.Clone(normalized.GetValue().GetPayload())
	wire.RequiredFeatures = slices.Clone(normalized.GetValue().GetRequiredFeatures())
	subject := referenceFromProtocol(normalized.GetNormalizedOwner())
	subject.DisplayName = ""
	projection := struct {
		Contract         string               `json:"contract"`
		Engine           string               `json:"engine"`
		Features         []string             `json:"features"`
		RequiredFeatures []string             `json:"required_features"`
		Subject          *modelReference      `json:"subject"`
		Format           string               `json:"format"`
		Projection       []byte               `json:"projection"`
		Required         []dependencyIdentity `json:"required"`
		Change           *changeFacts         `json:"change"`
		Operation        *operationFacts      `json:"operation"`
	}{m.signature,
		wire.Engine,
		featureSet(wire.Features),
		featureSet(wire.RequiredFeatures),
		subject,
		normalized.GetCanonicalFormat(),
		normalized.GetCanonicalProjection(),
		(&resolvedModel{normalized: normalized}).dependencyIdentities(),
		normalizedChangeFacts(normalized.GetChangeSemantics()), normalizedOperationFacts(normalized.GetOperationSemantics())}
	data, err := json.Marshal(projection)
	if err != nil {
		return nil, err
	}
	canonical, err := schemaext.CanonicalJSON(data)
	if err != nil {
		return nil, err
	}
	return &resolvedModel{kind: m.key.kind, signature: m.signature, wire: wire.clone(), canonical: canonical, normalized: proto.CloneOf(normalized)}, nil
}
