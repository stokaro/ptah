package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type modelPlanner struct {
	target      *target
	declaration *protocol.ModelPlanning
}

func (t *target) registerPlanning(result *engine.Provider) {
	for _, declaration := range t.engine.GetPlanning() {
		result.Planning = append(result.Planning, engine.Planning{
			Target:         t.engine.GetId(),
			Kinds:          boundKinds(declaration.GetChangeKinds()),
			ParentKinds:    boundKinds(declaration.GetParentKinds()),
			OperationKinds: boundKinds(declaration.GetOperationKinds()),
			Service:        modelPlanner{target: t, declaration: declaration},
		})
	}
}

// PlanFeatures sends a complete captured batch and binds the owner's validated
// result. The selected runtime and shared graph retain final planning authority.
func (s modelPlanner) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	remote, err := s.planningRequest(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	reply, err := s.target.host.client.PlanFeatures(ctx, remote)
	if err != nil {
		return featureplan.Result{}, err
	}
	result, err := s.target.planningReply(remote, reply)
	if err != nil {
		return featureplan.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func (s modelPlanner) planningRequest(request featureplan.Request) (*protocol.PlanFeaturesRequest, error) {
	if request.Target != s.target.engine.GetId() {
		return nil, fmt.Errorf("%w: planning target changed", schemaext.ErrInvalidValue)
	}
	if err := onlyFields(request, "Target", "Identifiers", "Capabilities", "Changes", "Tables", "Types", "CommonSteps", "ParentKinds", "DatabasePath"); err != nil {
		return nil, err
	}
	identifiers, err := comparisonIdentifiers(request.Identifiers)
	if err != nil {
		return nil, err
	}
	if err := request.Capabilities.Validate(); err != nil {
		return nil, err
	}
	remote := &protocol.PlanFeaturesRequest{
		Engine:       request.Target,
		PlanningId:   s.declaration.GetId(),
		Identifiers:  identifiers,
		Capabilities: make(map[string]bool),
		ParentKinds:  comparisonKinds(request.ParentKinds),
		DatabasePath: request.DatabasePath,
		DesiredTypes: planningTypes(request.Types.Desired),
		CurrentTypes: planningTypes(request.Types.Current),
	}
	for name, value := range request.Capabilities.All() {
		remote.Capabilities[string(name)] = value
	}
	capture := planningCapture{
		comparisonCapture: comparisonCapture{target: s.target, request: &protocol.CompareModelsRequest{Engine: request.Target, TargetFeatures: featureSet(s.target.engine.GetFeatures())}},
		identifiers:       request.Identifiers,
	}
	for i, change := range request.Changes {
		value, err := capture.value(schemaext.Change, change.Value, strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		remote.Changes = append(remote.Changes, &protocol.PlanningChange{Subject: comparisonIdentity(change.Subject), Value: value})
	}
	for _, table := range request.Tables {
		captured, err := capture.table(table)
		if err != nil {
			return nil, err
		}
		remote.Tables = append(remote.Tables, captured)
	}
	for _, step := range request.CommonSteps {
		captured, err := capture.commonStep(step)
		if err != nil {
			return nil, err
		}
		remote.CommonSteps = append(remote.CommonSteps, captured)
	}
	remote.TargetFeatures = slices.Clone(capture.request.GetTargetFeatures())
	return remote, nil
}

func planningTypes(types []schemaext.DefinedType) []*protocol.PlanningType {
	result := make([]*protocol.PlanningType, len(types))
	for i, value := range types {
		result[i] = &protocol.PlanningType{Identity: comparisonIdentity(value.ID), References: slices.Clone(value.References)}
	}
	return result
}
