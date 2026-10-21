package providerhost

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func (s modelComparer) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	remote, err := s.request(request.Target, request.Identifiers, request.Capabilities, request.Kinds)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	capture := comparisonCapture{target: s.target, request: remote}
	remote.Desired, err = capture.objects(schemaext.Desired, request.Desired)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	remote.Current, err = capture.objects(schemaext.Observed, request.Current)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	remote.Parents = comparisonParents(request.Parents)
	remote.DeclaredRelations, remote.DeclaredRoles, remote.Required = comparisonIdentities(request.DeclaredRelations), comparisonIdentities(request.DeclaredRoles), comparisonIdentities(request.Required)
	remote.DatabasePath = request.DatabasePath
	for _, action := range request.Requests {
		remote.Requests = append(remote.Requests, &protocol.RequestedChange{Subject: comparisonIdentity(action.Subject), Action: action.Action})
	}
	for _, exclusion := range request.Exclusions {
		remote.Exclusions = append(remote.Exclusions, &protocol.ComparisonExclusion{Kind: string(exclusion.Kind), Name: exclusion.Name})
	}
	for _, column := range request.DeclaredColumns {
		remote.DeclaredColumns = append(remote.DeclaredColumns, &protocol.ComparisonColumn{Table: comparisonIdentity(column.Table), Name: column.Name, Type: column.Type})
	}
	result, err := s.target.host.client.CompareModels(ctx, remote)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	desired, err := s.boundObjects(remote, result.GetDesired())
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	changes, err := s.boundChanges(remote, result.GetChanges())
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return schemaext.ObjectComparisonResult{Complete: true, Desired: desired, Changes: changes, Undecided: boundUndecided(result.GetUndecided())}, nil
}

func (s modelComparer) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	remote, err := s.request(request.Target, request.Identifiers, request.Capabilities, request.Kinds)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	capture := comparisonCapture{target: s.target, request: remote}
	remote.Desired, err = capture.facets(schemaext.Desired, request.Desired)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	remote.Current, err = capture.facets(schemaext.Observed, request.Current)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	remote.Parents = comparisonParents(request.Owners)
	remote.DesiredConstraints, remote.CurrentConstraints = comparisonConstraints(request.DesiredConstraints), comparisonConstraints(request.CurrentConstraints)
	result, err := s.target.host.client.CompareModels(ctx, remote)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	desired, err := s.boundFacets(remote, result.GetDesired(), request.Desired)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	changes, err := s.boundChanges(remote, result.GetChanges())
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	reply := schemaext.FacetComparisonResult{Complete: true, Desired: desired, Undecided: boundUndecided(result.GetUndecided())}
	for i, change := range changes {
		reply.Changes = append(reply.Changes, schemaext.FacetChange{Kind: schemaext.Kind(result.GetChanges()[i].GetKind()), Change: change})
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return reply, nil
}

func (t *target) registerComparisons(result *engine.Provider) {
	for _, declaration := range t.engine.GetComparisons() {
		service := modelComparer{target: t, declaration: declaration}
		switch declaration.GetPlacement() {
		case protocol.ModelComparison_OBJECTS:
			result.Comparisons = append(result.Comparisons,
				engine.ObjectComparison{Target: t.engine.GetId(),
					Kinds:       boundKinds(declaration.GetKinds()),
					ChangeKinds: boundKinds(declaration.GetChangeKinds()),
					Actions:     slices.Clone(declaration.GetActions()),
					Service:     service})
		case protocol.ModelComparison_FACETS:
			owners := make([]objectidentity.Kind, len(declaration.GetOwnerKinds()))
			for i, kind := range declaration.GetOwnerKinds() {
				owners[i] = objectidentity.Kind(kind)
			}
			result.FacetComparisons = append(result.FacetComparisons,
				engine.FacetComparison{Target: t.engine.GetId(),
					Kinds:       boundKinds(declaration.GetKinds()),
					ChangeKinds: boundKinds(declaration.GetChangeKinds()),
					OwnerKinds:  owners,
					Service:     service})
		}
	}
}
