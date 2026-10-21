package providerhost

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/schemaext"
)

// This host-owned persistence format contains source data, never a saved claim
// that the provider validated it. Decoding requires the selected owner's batch
// normalization service to reconstruct the local semantic projection.
type modelWire struct {
	Format           uint32          `json:"format"`
	Engine           string          `json:"engine"`
	Features         []string        `json:"features"`
	Subject          *modelReference `json:"subject"`
	RequiredFeatures []string        `json:"required_features"`
	Payload          []byte          `json:"payload"`
}

type modelReference struct {
	Engine      string   `json:"engine"`
	Database    string   `json:"database"`
	Catalog     string   `json:"catalog"`
	Schema      string   `json:"schema"`
	Path        []string `json:"path"`
	Kind        string   `json:"kind"`
	Name        string   `json:"name"`
	Overload    []string `json:"overload"`
	DisplayName string   `json:"display_name,omitempty"`
}

func referenceFromProtocol(ref *protocol.Reference) *modelReference {
	if ref == nil {
		return nil
	}
	return &modelReference{Engine: ref.GetEngine(), Database: ref.GetDatabase(), Catalog: ref.GetCatalog(), Schema: ref.GetSchema(),
		Path: append(make([]string, 0, len(ref.GetPath())), ref.GetPath()...), Kind: ref.GetKind(), Name: ref.GetName(),
		Overload: append(make([]string, 0, len(ref.GetOverload())), ref.GetOverload()...), DisplayName: ref.GetDisplayName()}
}

func (r *modelReference) protocol() *protocol.Reference {
	if r == nil {
		return nil
	}
	return &protocol.Reference{Engine: r.Engine, Database: r.Database, Catalog: r.Catalog, Schema: r.Schema, Path: slices.Clone(r.Path), Kind: r.Kind, Name: r.Name, Overload: slices.Clone(r.Overload), DisplayName: r.DisplayName}
}

func (w modelWire) clone() modelWire {
	w.Features = slices.Clone(w.Features)
	w.RequiredFeatures = slices.Clone(w.RequiredFeatures)
	w.Payload = slices.Clone(w.Payload)
	w.Subject = referenceFromProtocol(w.Subject.protocol())
	return w
}

func modelRepresentation(representation protocol.ValueInput_Representation) (schemaext.Representation, error) {
	switch representation {
	case protocol.ValueInput_DESIRED:
		return schemaext.Desired, nil
	case protocol.ValueInput_OBSERVED:
		return schemaext.Observed, nil
	case protocol.ValueInput_CHANGE:
		return schemaext.Change, nil
	default:
		return "", unsupported("native operation model adaptation")
	}
}

func protocolRepresentation(representation schemaext.Representation) protocol.ValueInput_Representation {
	switch representation {
	case schemaext.Desired:
		return protocol.ValueInput_DESIRED
	case schemaext.Observed:
		return protocol.ValueInput_OBSERVED
	case schemaext.Change:
		return protocol.ValueInput_CHANGE
	default:
		return protocol.ValueInput_REPRESENTATION_UNSPECIFIED
	}
}
