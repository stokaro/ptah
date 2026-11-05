package providerhost

import (
	"context"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
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
	return r.resolvedValues(batch, normalized)
}

// resolvedValues are a normalized batch's values as the host's models read
// them, each with the subject its owner names.
func (r reader) resolvedValues(batch *protocol.NormalizeValuesRequest, normalized *protocol.NormalizedValues) ([]observedValue, error) {
	h := r.session.target.host
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
	if source.GetKind() != "table" && source.GetKind() != "column" && source.GetKind() != "index" {
		return r.observedNativeIdentity(source, normalized)
	}
	if source.GetDatabase() != r.session.remote.GetDatabase() || normalized.GetDatabase() != source.GetDatabase() ||
		source.GetCatalog() != "" || normalized.GetCatalog() != "" || len(source.GetOverload()) != 0 || len(normalized.GetOverload()) != 0 {
		return objectidentity.ID{}, unsupported("common observed facet database, catalog, or overload identity")
	}
	semantics := r.session.info.IdentifierSemantics
	tableName, err := observedTableName(source, normalized, semantics)
	if err != nil {
		return objectidentity.ID{}, err
	}
	builder := objectidentity.NewBuilder(semantics)
	result := builder.TablePartsVerbatim(source.GetSchema(), tableName)
	normalizedSchema := normalized.GetSchema()
	if normalizedSchema == "" {
		normalizedSchema = semantics.TableIdentityKey(semantics.DefaultSchema)
	}
	if normalizedSchema != result.Schema.Normalized {
		return objectidentity.ID{}, unsupported("common observed facet schema normalization")
	}
	switch source.GetKind() {
	case "column":
		result.Kind, result.Parent = objectidentity.KindColumn, result.Name
		result.Name = objectidentity.Part{Source: source.GetName(), Normalized: semantics.ColumnIdentityKey(source.GetName())}
	case "index":
		// The index is named as the comparison names it, with the table as its
		// parent only where index names are scoped to the table.
		index := builder.IndexParts(source.GetSchema(), tableName, source.GetName())
		if index.Parent != (objectidentity.Part{}) {
			index.Parent = result.Name
		}
		index.Schema = result.Schema
		result = index
	}
	if normalized.GetName() != result.Name.Normalized {
		return objectidentity.ID{}, unsupported("common observed facet name normalization")
	}
	return result, nil
}

// observedTableName is the table a common observed facet belongs to: the
// owner itself, or the only path component of a column or an index, which the
// normalized owner must name by the table's identity key.
func observedTableName(source, normalized *protocol.Reference, semantics identifier.Semantics) (string, error) {
	if source.GetKind() == "column" || source.GetKind() == "index" {
		if len(source.GetPath()) != 1 || len(normalized.GetPath()) != 1 || normalized.GetPath()[0] != semantics.TableIdentityKey(source.GetPath()[0]) {
			return "", unsupported("common observed " + source.GetKind() + " parent identity")
		}
		return source.GetPath()[0], nil
	}
	if len(source.GetPath()) != 0 || len(normalized.GetPath()) != 0 {
		return "", unsupported("common observed table namespace")
	}
	return source.GetName(), nil
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

// planningContext reads a filtered snapshot's planning context: the objects of
// the provider's planning context kinds outside the read's scope, normalized as
// the snapshot's own values are, and what the read established about them
// across the database. A snapshot without one returns nothing.
func (r reader) planningContext(ctx context.Context, request *protocol.InspectRequest, snapshot *protocol.Schema) (schemaext.Objects, schemaext.Coverage, error) {
	h := r.session.target.host
	batch, err := provider.PlanningContextValues(h.description, request, snapshot)
	if err != nil || snapshot.GetPlanningContext() == nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	var objects []schemaext.Object
	if len(batch.GetValues()) != 0 {
		normalized, err := h.client.NormalizeValues(ctx, batch)
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		if err := provider.ValidateNormalizedValues(h.description, batch, normalized); err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		values, err := r.resolvedValues(batch, normalized)
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		for _, value := range values {
			objects = append(objects, schemaext.Object{Ref: value.subject, Value: value.value})
		}
	}
	result, err := schemaext.NewObjects(objects...)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	coverage, err := r.observedCoverage(snapshot.GetPlanningContext().GetCoverage())
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	return result, coverage, nil
}
