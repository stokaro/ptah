package providerhost

import (
	"context"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type observedValue struct {
	subject objectidentity.ID
	value   schemaext.Value
}

func (r reader) observedValues(ctx context.Context, request *protocol.InspectRequest, snapshot *protocol.Schema) ([]observedValue, error) {
	h := r.session.target.host
	batch, err := provider.InspectionValues(h.description, request, snapshot)
	if err != nil {
		return nil, err
	}
	if len(batch.GetValues()) == 0 {
		return nil, nil
	}
	normalized, err := h.client.NormalizeValues(ctx, batch)
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateNormalizedInspection(h.description, request, snapshot, normalized); err != nil {
		return nil, err
	}
	result := make([]observedValue, 0, len(normalized.GetValues()))
	for _, value := range normalized.GetValues() {
		kind := schemaext.Kind(value.GetValue().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Observed}]
		if contract == nil {
			return nil, fmt.Errorf("inspected model is not registered")
		}
		resolved, err := contract.resolved(modelWire{Format: 1, Engine: batch.GetEngine(), Features: batch.GetTargetFeatures()}, value)
		if err != nil {
			return nil, err
		}
		subject, err := r.observedIdentity(value.GetValue().GetOwner(), value.GetNormalizedOwner())
		if err != nil {
			return nil, err
		}
		result = append(result, observedValue{subject: subject, value: resolved})
	}
	return result, nil
}

func observedFacets(values []observedValue, engine string) (schemaext.Facets, error) {
	var result schemaext.Facets
	for _, value := range values {
		var err error
		result, err = result.With(value.value)
		if err != nil {
			return schemaext.Facets{}, err
		}
		result, err = result.WithTargetScope(value.value.Kind(), engine)
		if err != nil {
			return schemaext.Facets{}, err
		}
	}
	return result, nil
}

// Common identities omit the session database and encode a column's table as
// Parent. Check every discarded or moved component against the session's naming
// rules first. A selected native identity also omits the verified session
// database: its scope names an object within that database. The captured value
// retains the database binding as inspection evidence.
func (r reader) observedIdentity(source, normalized *protocol.Reference) (objectidentity.ID, error) {
	if source.GetKind() != "table" && source.GetKind() != "column" {
		return r.observedNativeIdentity(source, normalized)
	}
	if source.GetDatabase() != r.session.remote.GetDatabase() || normalized.GetDatabase() != source.GetDatabase() ||
		source.GetCatalog() != "" || normalized.GetCatalog() != "" || len(source.GetOverload()) != 0 || len(normalized.GetOverload()) != 0 {
		return objectidentity.ID{}, unsupported("common observed facet database, catalog, or overload identity")
	}
	semantics := r.session.info.IdentifierSemantics
	builder := objectidentity.NewBuilder(semantics)
	tableName := source.GetName()
	if source.GetKind() == "column" {
		if len(source.GetPath()) != 1 || len(normalized.GetPath()) != 1 || normalized.GetPath()[0] != semantics.TableIdentityKey(source.GetPath()[0]) {
			return objectidentity.ID{}, unsupported("common observed column parent identity")
		}
		tableName = source.GetPath()[0]
	} else if len(source.GetPath()) != 0 || len(normalized.GetPath()) != 0 {
		return objectidentity.ID{}, unsupported("common observed table namespace")
	}
	result := builder.TablePartsVerbatim(source.GetSchema(), tableName)
	normalizedSchema := normalized.GetSchema()
	if normalizedSchema == "" {
		normalizedSchema = semantics.TableIdentityKey(semantics.DefaultSchema)
	}
	if normalizedSchema != result.Schema.Normalized {
		return objectidentity.ID{}, unsupported("common observed facet schema normalization")
	}
	if source.GetKind() == "column" {
		result.Kind, result.Parent = objectidentity.KindColumn, result.Name
		result.Name = objectidentity.Part{Source: source.GetName(), Normalized: semantics.ColumnIdentityKey(source.GetName())}
	}
	if normalized.GetName() != result.Name.Normalized {
		return objectidentity.ID{}, unsupported("common observed facet name normalization")
	}
	return result, nil
}

func (r reader) observedNativeIdentity(source, normalized *protocol.Reference) (objectidentity.ID, error) {
	id, err := sourceIdentity(source, normalized)
	if err != nil {
		return objectidentity.ID{}, err
	}
	selected := r.session.target.host.selections
	id, err = selected.inSession(id, r.session.remote.GetDatabase())
	if err != nil {
		return objectidentity.ID{}, err
	}
	return id, selected.fit(id)
}
