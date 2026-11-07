package providerhost

import (
	"context"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

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
	remote.DeclaredRelations, remote.DeclaredRoles, remote.Required = s.declaredRelations(remote.GetTargetFeatures(), request.DeclaredRelations), comparisonIdentities(request.DeclaredRoles), comparisonIdentities(request.Required)
	remote.DeclaredObjects = s.declaredObjects(remote.GetTargetFeatures(), request.DeclaredObjects)
	remote.DeclaredFacets, err = s.declaredFacets(capture, request.DeclaredFacets)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	remote.DatabasePath = request.DatabasePath
	sent := s.writtenRequests(request.Requests)
	for _, index := range sent {
		action := request.Requests[index]
		if action.Written != "" {
			remote.Requests = append(remote.Requests, &protocol.RequestedChange{Written: action.Written, Action: action.Action})
			continue
		}
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
	resolved := make([]schemaext.ResolvedRequest, len(result.GetResolvedRequests()))
	for i, answer := range result.GetResolvedRequests() {
		resolved[i] = schemaext.ResolvedRequest{Request: sent[answer.GetRequest()], Subject: boundIdentity(answer.GetSubject())}
	}
	return schemaext.ObjectComparisonResult{Complete: true, Desired: desired, Changes: changes, Undecided: boundUndecided(result.GetUndecided()), Resolved: resolved,
		Counts: boundCounts(result.GetCounts())}, nil
}

// declaredRelations returns the declared relations a provider receives: every
// one when the provider and the engine negotiated
// provider.DeclaredRelationsFeature and the request's target features, which
// the captured values set, list it too; the views and materialized views
// alone otherwise, which is what the list holds for such a request. The SDK
// refuses a table the request's target does not list.
func (s modelComparer) declaredRelations(targetFeatures []string, relations []objectidentity.ID) []*protocol.ComparisonIdentity {
	if !s.target.negotiated(provider.DeclaredRelationsFeature) || !slices.Contains(targetFeatures, provider.DeclaredRelationsFeature) {
		relations = slices.DeleteFunc(slices.Clone(relations), func(relation objectidentity.ID) bool { return relation.Kind == objectidentity.KindTable })
	}
	return comparisonIdentities(relations)
}

// declaredObjects returns the declared objects a provider receives: every one
// the runtime gave a comparison whose declared kinds the description names,
// when the provider and the engine negotiated provider.DeclaredObjectsFeature
// and the request's target features list it too; none otherwise. The SDK
// keeps no declared kind for a host that did not list the feature, so the
// runtime gives such a comparison nothing to send.
func (s modelComparer) declaredObjects(targetFeatures []string, objects []objectidentity.ID) []*protocol.ComparisonIdentity {
	if len(objects) == 0 || !s.target.negotiated(provider.DeclaredObjectsFeature) || !slices.Contains(targetFeatures, provider.DeclaredObjectsFeature) {
		return nil
	}
	return comparisonIdentities(objects)
}

// declaredFacets returns the declared facets a provider receives: every facet
// the runtime gave a comparison whose declared facet kinds the description
// names, each with its desired value, when the provider and the engine
// negotiated provider.DeclaredFacetsFeature and the request's target features
// list it too; none otherwise. The SDK keeps no declared facet kind for a host
// that did not list the feature, so the runtime gives such a comparison
// nothing to send.
func (s modelComparer) declaredFacets(capture comparisonCapture, records []schemaext.FacetRecord) ([]*protocol.ComparisonRecord, error) {
	if len(records) == 0 || !s.target.negotiated(provider.DeclaredFacetsFeature) ||
		!slices.Contains(capture.request.GetTargetFeatures(), provider.DeclaredFacetsFeature) {
		return nil, nil
	}
	var result []*protocol.ComparisonRecord
	for _, record := range records {
		for _, kind := range record.Values.Kinds() {
			value, _, err := record.Values.Get(kind)
			if err != nil {
				return nil, err
			}
			captured, err := capture.value(schemaext.Desired, value, "declared-facet-"+strconv.Itoa(len(result)))
			if err != nil {
				return nil, err
			}
			result = append(result, &protocol.ComparisonRecord{Kind: string(kind), Subject: comparisonIdentity(record.Subject),
				Targets: record.Values.TargetScope(kind), Value: captured})
		}
	}
	return result, nil
}

// boundCounts converts the counts a provider's comparison stated. The SDK
// checked them against the reply, and the engine runtime checks them against
// its own capture of it. A provider that does not count sends none, so its
// kinds have no count and no inventory entry.
func boundCounts(counts []*protocol.ComparisonCount) []schemaext.ObjectCount {
	if len(counts) == 0 {
		return nil
	}
	result := make([]schemaext.ObjectCount, len(counts))
	for i, count := range counts {
		result[i] = schemaext.ObjectCount{Kind: schemaext.Kind(count.GetKind()), Matched: int(count.GetMatched()), Added: int(count.GetAdded()), Removed: int(count.GetRemoved())}
	}
	return result
}

// writtenRequests returns the indexes of the requests the provider receives,
// in order. A request that names its subject always travels. A written one
// travels only when the provider and the engine negotiated
// provider.PlanningPolicyFeature: a provider that did not cannot resolve it,
// and the runtime then refuses the spelling no owner resolved.
func (s modelComparer) writtenRequests(requests []schemaext.ChangeRequest) []int {
	sent := make([]int, 0, len(requests))
	for i, change := range requests {
		if change.Written == "" || s.target.negotiated(provider.PlanningPolicyFeature) {
			sent = append(sent, i)
		}
	}
	return sent
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
	reply := schemaext.FacetComparisonResult{Complete: true, Desired: desired, Undecided: boundUndecided(result.GetUndecided()), Counts: boundCounts(result.GetCounts())}
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
					Kinds:              boundKinds(declaration.GetKinds()),
					ChangeKinds:        boundKinds(declaration.GetChangeKinds()),
					Actions:            slices.Clone(declaration.GetActions()),
					DeclaredKinds:      boundKinds(declaration.GetDeclaredKinds()),
					DeclaredFacetKinds: boundKinds(declaration.GetDeclaredFacetKinds()),
					Service:            service})
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
