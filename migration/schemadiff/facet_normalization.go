package schemadiff

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// normalizeFeatureFacets binds common subjects to the connection before asking
// their owners for target facts. Pure comparison never receives the session.
func normalizeFeatureFacets(ctx context.Context, conn Connection, desired *schemamodel.Database, current *catalog.Database, info catalog.ServerInfo, runtime schemaext.FacetNormalizationService) (*schemamodel.Database, error) {
	if info.Dialect == "" || !slices.ContainsFunc(desired.FacetSlots(), func(facets *schemaext.Facets) bool { return !facets.IsZero() }) {
		return desired, nil
	}
	identity, err := comparisonTableIdentities(desired, current, &config.CompareOptions{Dialect: info.Dialect, IdentifierSemantics: &info.IdentifierSemantics})
	if err != nil {
		return nil, err
	}
	declared, observed, err := captureFeatureStates(identity.desired, identity.current, info.Dialect, identity.semantics)
	if err != nil {
		return nil, err
	}
	parents, err := featureParents(identity.desired, identity.current, info.Dialect, identity.semantics)
	if err != nil {
		return nil, err
	}
	result, err := runtime.NormalizeFacets(ctx, schemaext.FacetNormalizationRequest{
		Target: info.Dialect, Identifiers: identity.semantics, Capabilities: info.Capabilities,
		Desired: schemaext.FacetState{Records: declared.Facets, Coverage: declared.Coverage},
		Current: schemaext.FacetState{Records: observed.Facets, Coverage: observed.Coverage},
		Owners:  parents, Session: conn,
	})
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if !result.Complete {
		return nil, fmt.Errorf("%w: facet normalization did not complete", schemaext.ErrInvalidValue)
	}
	if !result.Desired.Coverage.Equal(declared.Coverage) {
		return nil, fmt.Errorf("%w: facet normalization changed source coverage", schemaext.ErrInvalidValue)
	}
	declared.Facets = result.Desired.Records
	return effectiveFeatureState(identity.desired, declared, info.Dialect, identity.semantics)
}
