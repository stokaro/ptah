package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// modelReverser is one reversal service an engine declares, which undoes the
// changes of its kinds the provider's comparisons returned.
type modelReverser struct {
	target      *target
	declaration *protocol.ModelReversal
}

// registerReversals registers each reversal service the engine declares as
// the reversal owner of its change kinds. The negotiated description lists
// none unless the host lists provider.ReverseChangesFeature.
func (t *target) registerReversals(result *engine.Provider) {
	for _, declaration := range t.engine.GetReversals() {
		result.Reversals = append(result.Reversals, engine.Reversal{
			Target: t.engine.GetId(), Kinds: boundKinds(declaration.GetChangeKinds()), Service: modelReverser{target: t, declaration: declaration},
		})
	}
}

// ReverseChanges sends the accepted changes the runtime routed to this
// service and binds the provider's validated results to the same subjects.
// The engine runtime checks the results against its codecs before any
// planner reads them.
func (s modelReverser) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	remote, profile, err := s.reversalRequest(request)
	if err != nil {
		return nil, err
	}
	reply, err := s.target.host.client.ReverseChanges(ctx, remote)
	if err != nil {
		return nil, err
	}
	result, err := s.boundReversals(profile, request, reply)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// reversalRequest captures the changes in the wire form the comparison
// returned them in, under the target profile they were captured with, which
// the results are bound under too.
func (s modelReverser) reversalRequest(request schemaext.ReversalRequest) (*protocol.ReverseChangesRequest, *protocol.CompareModelsRequest, error) {
	profile, err := modelComparer{target: s.target}.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return nil, nil, err
	}
	capture := comparisonCapture{target: s.target, request: profile}
	remote := &protocol.ReverseChangesRequest{Engine: profile.GetEngine(), ReversalId: s.declaration.GetId(), Identifiers: profile.GetIdentifiers(), Capabilities: profile.GetCapabilities()}
	for i, change := range request.Changes {
		value, err := capture.value(schemaext.Change, change.Value, strconv.Itoa(i))
		if err != nil {
			return nil, nil, err
		}
		remote.Changes = append(remote.Changes, &protocol.PlanningChange{Subject: comparisonIdentity(change.Subject), Value: value})
	}
	remote.TargetFeatures = slices.Clone(profile.GetTargetFeatures())
	return remote, profile, nil
}

// boundReversals binds each result to the change it undoes. The SDK client
// has checked that a result keeps the subject it was sent, so the change
// keeps the runtime's own identity.
func (s modelReverser) boundReversals(profile *protocol.CompareModelsRequest, request schemaext.ReversalRequest, reply *protocol.ReversedChanges) ([]schemaext.Reversal, error) {
	if len(reply.GetChanges()) != len(request.Changes) {
		return nil, fmt.Errorf("%w: reversal changed the result count", schemaext.ErrInvalidValue)
	}
	comparer := modelComparer{target: s.target}
	result := make([]schemaext.Reversal, len(reply.GetChanges()))
	for i, reversed := range reply.GetChanges() {
		reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: request.Changes[i].Subject},
			Strategy: reversed.GetStrategy(), Limitations: slices.Clone(reversed.GetLimitations())}
		if value := reversed.GetValue(); value != nil {
			model, err := comparer.resolved(profile, schemaext.Change, value)
			if err != nil {
				return nil, err
			}
			reversal.Change.Value = &resolvedChange{model: model}
		}
		for _, state := range reversed.GetForwardState() {
			projected, err := forwardState(comparer, profile, state)
			if err != nil {
				return nil, err
			}
			reversal.ForwardState = append(reversal.ForwardState, projected)
		}
		result[i] = reversal
	}
	return result, nil
}

// forwardState binds one predicted state. A state without a value is the
// established absence of one.
func forwardState(comparer modelComparer, profile *protocol.CompareModelsRequest, state *protocol.ForwardState) (schemaext.ProjectedValue, error) {
	placements := map[protocol.ForwardState_Placement]schemaext.Placement{protocol.ForwardState_OBJECT: schemaext.ObjectPlacement, protocol.ForwardState_FACET: schemaext.FacetPlacement}
	placement, known := placements[state.GetPlacement()]
	if !known {
		return schemaext.ProjectedValue{}, fmt.Errorf("%w: unknown forward state placement", schemaext.ErrInvalidValue)
	}
	projected := schemaext.ProjectedValue{Placement: placement, Kind: schemaext.Kind(state.GetKind())}
	if state.GetValue() == nil {
		return projected, nil
	}
	value, err := comparer.resolved(profile, schemaext.Observed, state.GetValue())
	if err != nil {
		return schemaext.ProjectedValue{}, err
	}
	projected.Value = value
	return projected, nil
}
