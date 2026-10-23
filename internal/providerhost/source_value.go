package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// decodedSourceValue is a value a provider decoded from a source document,
// resolved against the host's model contract. Every source format places it the
// same way, so the format-specific code only decides where it attaches.
type decodedSourceValue struct {
	kind    schemaext.Kind
	subject objectidentity.ID
	value   schemaext.Value
}

func (h *Host) decodedValue(engine string, source *protocol.SourceModelValue) (decodedSourceValue, error) {
	value := source.GetValue()
	kind := schemaext.Kind(value.GetValue().GetKind())
	contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
	if contract == nil {
		return decodedSourceValue{}, fmt.Errorf("decoded source model is not registered")
	}
	resolved, err := contract.resolved(modelWire{Format: 1, Engine: engine}, value)
	if err != nil {
		return decodedSourceValue{}, err
	}
	subject, err := sourceIdentity(value.GetValue().GetOwner(), value.GetNormalizedOwner())
	if err != nil {
		return decodedSourceValue{}, err
	}
	return decodedSourceValue{kind: kind, subject: subject, value: resolved}, nil
}

// requireTable refuses a facet whose owner is qualified beyond a schema and a
// name, because a table facet attaches to a table of the document by those two.
func (v decodedSourceValue) requireTable(format string) error {
	if !v.subject.Database.Empty() || !v.subject.Catalog.Empty() || !v.subject.Namespace.IsZero() || !v.subject.Signature.IsZero() {
		return unsupported("native " + format + " table facet with database, catalog, path, or overload qualification")
	}
	return nil
}
