package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validatePlanningContributions(description *protocol.Description, request *protocol.PlanFeaturesRequest, service *protocol.ModelPlanning, contributions []*protocol.PlanningContribution) (map[planningStepKey]bool, error) {
	steps := make(map[planningStepKey]bool)
	common := make(map[planningStepKey]bool)
	for _, step := range request.GetCommonSteps() {
		common[stepKey(step.GetId())] = true
	}
	values := make([]*protocol.NormalizedValue, 0)
	for _, contribution := range contributions {
		if contribution.GetOwner() != description.GetProviderId() {
			return nil, fmt.Errorf("%w: unowned planning contribution", ErrProtocol)
		}
		for _, step := range contribution.GetSteps() {
			if err := validateStepIdentity(step.GetId()); err != nil {
				return nil, err
			}
			key := stepKey(step.GetId())
			_, duplicate := steps[key]
			if key.owner != contribution.GetOwner() || duplicate || common[key] {
				return nil, fmt.Errorf("%w: duplicate or unowned planning step", ErrProtocol)
			}
			steps[key] = false
			// Map presence and receipt coverage are separate; false means unaccounted.
			if err := validatePlanningMetadata(step.GetMetadata()); err != nil {
				return nil, err
			}
			if err := validatePlanningOperation(request, service, step); err != nil {
				return nil, err
			}
			if step.GetOperation().GetPhase() == protocol.PlanningOperation_LIFECYCLE && !planningLifecycleAvailable(description, request.GetEngine()) {
				return nil, fmt.Errorf("%w: lifecycle planning was not negotiated", ErrProtocol)
			}
			values = append(values, step.GetOperation().GetValue())
		}
		if err := validatePlanningDependencies(contribution.GetDependencies()); err != nil {
			return nil, err
		}
	}
	if err := validateComparisonValues(description, planningValueContext(request), protocol.ValueInput_OPERATION, values); err != nil {
		return nil, err
	}
	return steps, nil
}

func validatePlanningOperation(request *protocol.PlanFeaturesRequest, service *protocol.ModelPlanning, step *protocol.PlanningStep) error {
	operation := step.GetOperation()
	if !slices.Contains(service.GetOperationKinds(), operation.GetValue().GetValue().GetKind()) || operation.GetValue().GetId() != step.GetId().GetName() {
		return fmt.Errorf("%w: planning operation model or identity changed", ErrProtocol)
	}
	if _, ok := protocol.PlanningOperation_Phase_name[int32(operation.GetPhase())]; !ok {
		return fmt.Errorf("%w: unknown planning phase", ErrProtocol)
	}
	switch operation.GetRole() {
	case protocol.NativeOperation_STATEMENT:
		if operation.GetParent() != nil {
			return fmt.Errorf("%w: standalone planning operation has a parent", ErrProtocol)
		}
	case protocol.NativeOperation_ALTER_TABLE:
		if err := ValidateComparisonIdentity(operation.GetParent()); err != nil {
			return err
		}
		found := slices.ContainsFunc(request.GetTables(), func(table *protocol.PlanningTable) bool {
			return CompareIdentities(table.GetSubject(), operation.GetParent()) == 0
		})
		if !found {
			return fmt.Errorf("%w: planning operation names an uncaptured parent", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: unknown planning operation role", ErrProtocol)
	}
	return nil
}

func validatePlanningReceipts(request *protocol.PlanFeaturesRequest, result *protocol.PlannedFeatures, steps map[planningStepKey]bool) error {
	if len(result.GetChanges()) != len(request.GetChanges()) {
		return fmt.Errorf("%w: missing planning change receipts", ErrProtocol)
	}
	for i, change := range request.GetChanges() {
		if err := validatePlanningReceipt(result.GetChanges()[i], change.GetSubject(), change.GetValue().GetValue().GetKind(), steps); err != nil {
			return err
		}
	}
	index := 0
	for _, table := range request.GetTables() {
		if table.GetAction() == protocol.PlanningTable_CONTEXT {
			continue
		}
		for _, kind := range request.GetParentKinds() {
			if index >= len(result.GetParents()) {
				return fmt.Errorf("%w: missing planning parent receipt", ErrProtocol)
			}
			receipt := result.GetParents()[index]
			if receipt.GetAction() != table.GetAction() {
				return fmt.Errorf("%w: planning parent action changed", ErrProtocol)
			}
			if err := validatePlanningReceipt(receipt.GetReceipt(), table.GetSubject(), kind, steps); err != nil {
				return err
			}
			index++
		}
	}
	if index != len(result.GetParents()) {
		return fmt.Errorf("%w: extra planning parent receipts", ErrProtocol)
	}
	if err := validatePlanningObjectReceipts(request, result, steps); err != nil {
		return err
	}
	for _, accounted := range steps {
		if !accounted {
			return fmt.Errorf("%w: planning operation has no receipt", ErrProtocol)
		}
	}
	return nil
}

func validatePlanningReceipt(receipt *protocol.PlanningReceipt, subject *protocol.ComparisonIdentity, kind string, steps map[planningStepKey]bool) error {
	if !proto.Equal(receipt.GetSubject(), subject) || receipt.GetKind() != kind || !comparisonReason(receipt.GetStrategy()) {
		return fmt.Errorf("%w: planning receipt changed its input or omitted its strategy", ErrProtocol)
	}
	seen := make(map[planningStepKey]bool)
	for _, step := range receipt.GetSteps() {
		key := stepKey(step)
		if _, found := steps[key]; !found || seen[key] {
			return fmt.Errorf("%w: planning receipt names a missing or duplicate step", ErrProtocol)
		}
		seen[key], steps[key] = true, true
	}
	return nil
}

func validatePlanningRewrites(request *protocol.PlanFeaturesRequest, result *protocol.PlannedFeatures, steps map[planningStepKey]bool) error {
	common := make(map[planningStepKey]bool)
	for _, step := range request.GetCommonSteps() {
		common[stepKey(step.GetId())] = len(step.GetMetadata().GetEffects()) > 0
	}
	claimed := make(map[planningStepKey]bool)
	for _, rewrite := range result.GetRewrites() {
		if _, found := steps[stepKey(rewrite.GetReplacement())]; !found || len(rewrite.GetSources()) == 0 {
			return fmt.Errorf("%w: invalid planning rewrite replacement", ErrProtocol)
		}
		for _, source := range rewrite.GetSources() {
			key := stepKey(source)
			if !common[key] || claimed[key] {
				return fmt.Errorf("%w: planning rewrite claims a missing, unknown, or already claimed footprint", ErrProtocol)
			}
			claimed[key] = true
		}
	}
	return nil
}

func validatePlanningDependencies(dependencies []*protocol.PlanningDependency) error {
	for _, dependency := range dependencies {
		if err := validateStepIdentity(dependency.GetBefore()); err != nil {
			return err
		}
		if err := validateStepIdentity(dependency.GetAfter()); err != nil {
			return err
		}
		if stepKey(dependency.GetBefore()) == stepKey(dependency.GetAfter()) {
			return fmt.Errorf("%w: planning step depends on itself", ErrProtocol)
		}
	}
	return nil
}
