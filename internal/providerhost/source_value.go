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
	selected := h.targets[engine]
	if selected == nil {
		return decodedSourceValue{}, fmt.Errorf("decoded source value names engine %q, which the provider does not declare", engine)
	}
	// A value carries the feature profile of the engine it was decoded for, as
	// every other value does. A comparison or a plan accepts one profile per
	// batch, and a value without one would set the batch's profile to none.
	resolved, err := contract.resolved(modelWire{Format: 1, Engine: engine, Features: featureSet(selected.engine.GetFeatures())}, value)
	if err != nil {
		return decodedSourceValue{}, err
	}
	subject, err := sourceIdentity(value.GetValue().GetOwner(), value.GetNormalizedOwner())
	if err != nil {
		return decodedSourceValue{}, err
	}
	if err := h.selections.fit(subject); err != nil {
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
