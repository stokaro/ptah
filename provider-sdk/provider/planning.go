package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// PlanFeaturesFeature advertises offline planning against captured context.
const PlanFeaturesFeature = "models.plan"

// FeaturePlanner contributes complete operations and receipts. The host joins
// contributions to its common graph before rendering or executing any step.
// Implementations honor cancellation and never fetch missing source or database
// state. A completed refusal returns diagnostics without successful output.
type FeaturePlanner interface {
	PlanFeatures(context.Context, *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error)
}

// ValidatePlanningRequest checks service ownership and all captured context
// before owner code runs. Missing knowledge remains unknown.
func ValidatePlanningRequest(description *protocol.Description, request *protocol.PlanFeaturesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), PlanFeaturesFeature) {
		return fmt.Errorf("%w: feature planning is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelPlanning(description, engine); err != nil {
		return err
	}
	service := findModelPlanning(description, request.GetEngine(), request.GetPlanningId())
	if service == nil {
		return fmt.Errorf("%w: planning service is unavailable", ErrProtocol)
	}
	// Registration assigns parent models; a caller cannot silently narrow them.
	expected := service.GetParentKinds()
	if !hasPlanningActions(request) {
		expected = nil
	}
	if !slices.Equal(request.GetParentKinds(), expected) {
		return fmt.Errorf("%w: planning parent assignment changed", ErrProtocol)
	}
	if err := validatePlanningContext(request); err != nil {
		return err
	}
	if err := validatePlanningChanges(description, request, service); err != nil {
		return err
	}
	return validatePlanningTables(description, request, engine)
}

func hasPlanningActions(request *protocol.PlanFeaturesRequest) bool {
	for _, table := range request.GetTables() {
		if table.GetAction() != protocol.PlanningTable_CONTEXT {
			return true
		}
	}
	return false
}

func validatePlanningChanges(description *protocol.Description, request *protocol.PlanFeaturesRequest, service *protocol.ModelPlanning) error {
	values := make([]*protocol.NormalizedValue, 0, len(request.GetChanges()))
	records := make([]*protocol.ComparisonRecord, 0, len(request.GetChanges()))
	for _, change := range request.GetChanges() {
		kind := change.GetValue().GetValue().GetKind()
		if !slices.Contains(service.GetChangeKinds(), kind) {
			return fmt.Errorf("%w: unowned planning change", ErrProtocol)
		}
		if err := ValidateComparisonIdentity(change.GetSubject()); err != nil {
			return err
		}
		records = append(records, &protocol.ComparisonRecord{Kind: kind, Subject: change.GetSubject()})
		values = append(values, change.GetValue())
	}
	slices.SortFunc(records, compareComparisonRecords)
	for i := 1; i < len(records); i++ {
		if compareComparisonRecords(records[i-1], records[i]) == 0 {
			return fmt.Errorf("%w: duplicate planning change", ErrProtocol)
		}
	}
	return validateComparisonValues(description, planningValueContext(request), protocol.ValueInput_CHANGE, values)
}

func planningValueContext(request *protocol.PlanFeaturesRequest) *protocol.CompareModelsRequest {
	return &protocol.CompareModelsRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}
}

// ValidatePlannedFeatures refuses incomplete replies, unowned operations, missing
// receipts, and successful output mixed with a refusal. External dependencies
// and rewrite effects are validated again against the host's complete graph.
func ValidatePlannedFeatures(description *protocol.Description, request *protocol.PlanFeaturesRequest, result *protocol.PlannedFeatures) error {
	if err := ValidatePlanningRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() {
		return fmt.Errorf("%w: incomplete feature planning", ErrProtocol)
	}
	if len(result.GetDiagnostics()) > 0 {
		return validatePlanningDiagnostics(request, result)
	}
	service := findModelPlanning(description, request.GetEngine(), request.GetPlanningId())
	steps, err := validatePlanningContributions(description, request, service, result.GetContributions())
	if err != nil {
		return err
	}
	if err := validatePlanningReceipts(request, result, steps); err != nil {
		return err
	}
	return validatePlanningRewrites(request, result, steps)
}
