package providerhost

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/schemaext"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// headerCodecs reads and writes the captured feature coverage header of an
// HCL document, which states what the document knows of the provider's
// desired models.
//
// A header names each model by the identity the provider describes: the
// provider's id as the owner, the model's kind and representation, its schema
// version and its definition digest (ADR 0084). Everything else the host
// holds of the model names it by the identity of the host's own codec, whose
// definition binds the provider's release and what the host negotiated with
// it, so a saved plan applies only with the release that planned it. A header
// written under that identity would be refused by the next release of the
// provider even when no model changed, so the two identities meet only here.
type headerCodecs struct {
	// registry holds a codec of each desired model under the identity the
	// provider describes, with the predecessors the engines name for it.
	registry schemaext.Registry
	// held is the identity of the host's own codec of each desired model.
	held map[schemaext.Kind]schemaext.CodecIdentity
}

// headerCodecs builds the codecs of the provider's captured headers. The
// codecs decode through the provider's batch decoder, as the runtime
// registers them (see Provider), since a registry refuses a codec that has no
// decoder; a header never decodes a value. Each codec carries the earlier
// identities the engines name for its model (provider.ModelPredecessorsFeature
// and provider.OwnPredecessorsFeature), so a document captured under one, such
// as an export of a former built-in engine or of a release before a definition
// text changed, makes the same claim.
func (h *Host) headerCodecs() (headerCodecs, error) {
	var owned []schemaext.OwnedCodec
	described := make(map[schemaext.Kind]*protocol.ValueModel)
	held := make(map[schemaext.Kind]schemaext.CodecIdentity)
	decoder := &schemaext.BatchDecoder{Decode: h.decodeModels}
	for _, codec := range h.modelCodecs {
		if codec.Representation != schemaext.Desired {
			continue
		}
		kind := codec.Prototype.Kind()
		contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
		model, err := describedModel(contract)
		if err != nil {
			return headerCodecs{}, err
		}
		described[kind] = model
		held[kind] = schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind, Representation: schemaext.Desired, Version: codec.Version,
			Definition: contract.signature}
		claimed := codec
		claimed.Version, claimed.Definition = model.GetSchemaVersion(), model.GetDefinitionJson()
		owned = append(owned, schemaext.OwnedCodec{Owner: h.description.GetProviderId(), Codec: claimed, BatchDecoder: decoder,
			Predecessors: h.desiredPredecessors(kind)})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return headerCodecs{}, err
	}
	for _, kind := range slices.Sorted(maps.Keys(described)) {
		identity, _ := registry.Identity(kind, schemaext.Desired)
		if identity.Definition != "sha256:"+described[kind].GetDefinitionSha256() {
			return headerCodecs{}, fmt.Errorf("%w: the definition of model %q does not have the digest provider %s describes", provider.ErrProtocol, kind,
				h.description.GetProviderId())
		}
	}
	return headerCodecs{registry: registry, held: held}, nil
}

// describedModel is the model every engine of contract describes. A model two
// engines describe with another schema version or definition has no one
// identity a header can name it by.
func describedModel(contract *modelContract) (*protocol.ValueModel, error) {
	var model *protocol.ValueModel
	for _, engine := range slices.Sorted(maps.Keys(contract.engines)) {
		candidate := contract.engines[engine]
		if model != nil && (candidate.GetSchemaVersion() != model.GetSchemaVersion() || candidate.GetDefinitionSha256() != model.GetDefinitionSha256()) {
			return nil, unsupported(fmt.Sprintf("a feature coverage header of model %q, which the provider's engines define differently", contract.key.kind))
		}
		model = candidate
	}
	return model, nil
}

// desiredPredecessors are the earlier identities the provider's engines name
// for the desired model of kind, each once, in the order the engines name
// them.
func (h *Host) desiredPredecessors(kind schemaext.Kind) []schemaext.CodecIdentity {
	var identities []schemaext.CodecIdentity
	for _, engine := range h.description.GetEngines() {
		for _, predecessor := range engine.GetModelPredecessors() {
			if schemaext.Kind(predecessor.GetKind()) != kind || predecessor.GetRepresentation() != protocol.ValueInput_DESIRED {
				continue
			}
			identity := schemaext.CodecIdentity{Owner: predecessor.GetFormerOwner(), Kind: schemaext.Kind(predecessor.GetFormerKind()),
				Representation: schemaext.Desired, Version: predecessor.GetSchemaVersion(), Definition: "sha256:" + predecessor.GetDefinitionSha256()}
			if !slices.Contains(identities, identity) {
				identities = append(identities, identity)
			}
		}
	}
	return identities
}

// read restores the claims of a document's captured header bodies, each
// named by the identity of the host's own codec of its model. A header that
// names a model under an identity the provider neither describes nor names
// as a predecessor is refused.
func (c headerCodecs) read(ctx context.Context, bodies []string) (schemaext.Coverage, error) {
	known, _, err := c.registry.DecodeCoverageHeaderBodies(ctx, schemaext.Desired, bodies)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return renamed(known, func(record schemaext.KindCoverage) (schemaext.CodecIdentity, bool) {
		identity, found := c.held[record.Model.Kind]
		return identity, found
	})
}

// line is the captured header body that states known, or none for a document
// that knows nothing of the provider's models. A record that names a model
// by the host's codec is written under the identity the provider describes;
// any other is left as it is, for the registry to refuse.
func (c headerCodecs) line(ctx context.Context, known schemaext.Coverage) (string, error) {
	written, err := renamed(known, func(record schemaext.KindCoverage) (schemaext.CodecIdentity, bool) {
		if record.Model != c.held[record.Model.Kind] {
			return schemaext.CodecIdentity{}, false
		}
		return c.registry.Identity(record.Model.Kind, schemaext.Desired)
	})
	if err != nil {
		return "", err
	}
	return c.registry.EncodeCoverageHeader(ctx, schemaext.Desired, written)
}

// renamed is known with each kind record named by the identity identity
// returns for it, where it returns one. Subject records name a kind, not an
// identity, and are kept.
func renamed(known schemaext.Coverage, identity func(schemaext.KindCoverage) (schemaext.CodecIdentity, bool)) (schemaext.Coverage, error) {
	if known.IsZero() {
		return known, nil
	}
	kinds := known.KindRecords()
	for i, record := range kinds {
		if model, found := identity(record); found {
			kinds[i].Model = model
		}
	}
	return schemaext.NewCoverage(known.Representation(), kinds, known.SubjectRecords())
}
