package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// CompareModelsFeature advertises contextual comparison of native objects or
// attached facets. Canonical equality alone does not implement this service.
const CompareModelsFeature = "models.compare"

// ModelComparer interprets captured states and context without opening a session.
// It validates payload semantics, preserves unknown coverage and exclusions, and
// returns a complete result or an error. The host owns common migration policy.
type ModelComparer interface {
	CompareModels(context.Context, *protocol.CompareModelsRequest) (*protocol.ComparedModels, error)
}

// ValidateComparisonRequest checks negotiated ownership and captured context
// before dispatch. It does not infer absence from an empty object collection.
func ValidateComparisonRequest(description *protocol.Description, request *protocol.CompareModelsRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), CompareModelsFeature) {
		return fmt.Errorf("%w: model comparison is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelComparisons(description, engine); err != nil {
		return err
	}
	service := findModelComparison(description, request.GetEngine(), request.GetComparisonId())
	if service == nil || len(request.GetKinds()) == 0 {
		return fmt.Errorf("%w: comparison owner or models are missing", ErrProtocol)
	}
	if err := validateComparisonNames(request.GetKinds()); err != nil {
		return err
	}
	for _, kind := range request.GetKinds() {
		if !slices.Contains(service.GetKinds(), kind) {
			return fmt.Errorf("%w: comparison model is not owned by this service", ErrProtocol)
		}
	}
	if err := validateComparisonContext(request, service); err != nil {
		return err
	}
	if err := validateWrittenRequests(description, engine, request); err != nil {
		return err
	}
	if err := validateComparisonState(description, request, service, protocol.ValueInput_DESIRED, request.GetDesired()); err != nil {
		return err
	}
	return validateComparisonState(description, request, service, protocol.ValueInput_OBSERVED, request.GetCurrent())
}

// ValidateComparedModels refuses partial replies, foreign models, and changes
// to excluded facets. Engine validation subsequently enforces migration policy.
func ValidateComparedModels(description *protocol.Description, request *protocol.CompareModelsRequest, result *protocol.ComparedModels) error {
	if err := ValidateComparisonRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() {
		return fmt.Errorf("%w: incomplete model comparison", ErrProtocol)
	}
	service := findModelComparison(description, request.GetEngine(), request.GetComparisonId())
	if err := validateComparisonState(description, request, service, protocol.ValueInput_DESIRED, result.GetDesired()); err != nil {
		return err
	}
	if err := validateComparisonExclusions(request, result); err != nil {
		return err
	}
	if err := validateComparedChanges(description, request, service, result.GetChanges()); err != nil {
		return err
	}
	// ValidateComparisonRequest resolved the engine.
	engine, _, _ := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err := validateResolvedRequests(description, engine, request, result); err != nil {
		return err
	}
	for _, diagnostic := range result.GetUndecided() {
		if !slices.Contains(request.GetKinds(), diagnostic.GetKind()) || !comparisonReason(diagnostic.GetReason()) {
			return fmt.Errorf("%w: undecided comparison requires an owned model and reason", ErrProtocol)
		}
		if diagnostic.GetSubject() == nil && service.GetPlacement() == protocol.ModelComparison_OBJECTS {
			continue
		}
		if err := ValidateComparisonIdentity(diagnostic.GetSubject()); err != nil {
			return err
		}
	}
	return nil
}
