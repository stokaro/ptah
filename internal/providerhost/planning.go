package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

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
			ContextKinds:   boundKinds(declaration.GetContextKinds()),
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
	if err := remapObjectDiagnostics(remote, request.Objects.AllSubjects(), &result); err != nil {
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
	if err := onlyFields(request, "Target", "Identifiers", "Capabilities", "Changes", "Tables", "Types", "CommonSteps", "ParentKinds", "DatabasePath", "Policy", "Objects", "ContextKinds"); err != nil {
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
		Engine:          request.Target,
		PlanningId:      s.declaration.GetId(),
		Identifiers:     identifiers,
		Capabilities:    make(map[string]bool),
		ParentKinds:     comparisonKinds(request.ParentKinds),
		ContextKinds:    comparisonKinds(request.ContextKinds),
		ChangedSubjects: comparisonIdentities(request.Objects.ChangedSubjects),
		DatabasePath:    request.DatabasePath,
		DesiredTypes:    planningTypes(request.Types.Desired),
		CurrentTypes:    planningTypes(request.Types.Current),
	}
	for name, value := range request.Capabilities.All() {
		remote.Capabilities[string(name)] = value
	}
	capture := planningCapture{
		comparisonCapture: comparisonCapture{target: s.target, request: &protocol.CompareModelsRequest{Engine: request.Target, TargetFeatures: featureSet(s.target.engine.GetFeatures())}},
		identifiers:       request.Identifiers,
	}
	if len(request.ContextKinds) != 0 {
		remote.DesiredObjects, err = capture.objects(schemaext.Desired, request.Objects.Desired)
		if err != nil {
			return nil, err
		}
		remote.CurrentObjects, err = capture.objects(schemaext.Observed, request.Objects.Current)
		if err != nil {
			return nil, err
		}
		canonicalizePlanningObjectDatabases(remote)
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
	remote.Policy, err = s.target.planningPolicy(request.Policy)
	if err != nil {
		return nil, err
	}
	return remote, nil
}

// planningPolicy is the policy a provider's planner receives: none unless the
// provider and the engine list provider.PlanningPolicyFeature. A planner that
// does not honor a policy plans as if none were set, which is how the host's
// own planners treat a request they have no behavior for, so the host does
// not refuse the operator's flags for it.
func (t *target) planningPolicy(policy featureplan.Policy) (*protocol.PlanningPolicy, error) {
	if !policy.Statements.Valid() {
		return nil, fmt.Errorf("%w: unknown statement style %d", schemaext.ErrInvalidValue, policy.Statements)
	}
	if policy.Statements == featureplan.StatementStyleExact && !t.negotiated(provider.PlanningStatementStyleFeature) {
		return nil, fmt.Errorf("%w: exact planning needs %s", schemaext.ErrInvalidValue, provider.PlanningStatementStyleFeature)
	}
	if !t.negotiated(provider.PlanningPolicyFeature) {
		return nil, nil
	}
	style := protocol.PlanningStatementStyle_PLANNING_STATEMENT_STYLE_ANNOTATED
	if policy.Statements == featureplan.StatementStyleExact {
		style = protocol.PlanningStatementStyle_PLANNING_STATEMENT_STYLE_EXACT
	}
	return &protocol.PlanningPolicy{
		StatementStyle:               style,
		AllowTableRebuild:            policy.AllowTableRebuild,
		TableRebuildRequest:          policy.TableRebuildRequest,
		OnlineAlter:                  policy.OnlineAlter,
		ConcurrentIndexBuilds:        policy.ConcurrentIndexBuilds,
		ConcurrentIndexBuildSubjects: comparisonIdentities(policy.ConcurrentIndexBuildSubjects),
		ConcurrentIndexDrops:         policy.ConcurrentIndexDrops,
		ConcurrentIndexDropSubjects:  comparisonIdentities(policy.ConcurrentIndexDropSubjects),
		OmitNullBackfill:             policy.OmitNullBackfill,
	}, nil
}

func planningTypes(types []schemaext.DefinedType) []*protocol.PlanningType {
	result := make([]*protocol.PlanningType, len(types))
	for i, value := range types {
		result[i] = &protocol.PlanningType{Identity: comparisonIdentity(value.ID), References: slices.Clone(value.References)}
	}
	return result
}
