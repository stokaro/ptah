package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// PlanningObjectContextFeature enables complete native object context and
// explicit assessment receipts, including objects without a schema difference.
const PlanningObjectContextFeature = "planning.object-context"

func planningObjectError(reason string) error {
	return fmt.Errorf("%w: native planning context: %s", ErrProtocol, reason)
}

func negotiatePlanningObjects(engine *protocol.Engine, features []string) {
	if slices.Contains(features, PlanningObjectContextFeature) {
		return
	}
	for _, service := range engine.GetPlanning() {
		service.ContextKinds = nil
	}
	engine.Planning = slices.DeleteFunc(engine.Planning, func(service *protocol.ModelPlanning) bool {
		return len(service.GetChangeKinds())+len(service.GetParentKinds()) == 0
	})
}

func validatePlanningObjectAssignments(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), PlanningObjectContextFeature) &&
		(!EngineOffers(description, engine.GetId(), PlanningObjectContextFeature) || !slices.Contains(description.GetFeatures(), PlanFeaturesFeature)) {
		return planningObjectError("the engine requires the negotiated planning service")
	}
	seen := make(map[string]bool)
	for _, service := range engine.GetPlanning() {
		if len(service.GetContextKinds()) != 0 && !EngineOffers(description, engine.GetId(), PlanningObjectContextFeature) {
			return planningObjectError("assessment assignment was not negotiated")
		}
		for _, kind := range service.GetContextKinds() {
			if seen[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED) == nil ||
				findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OBSERVED) == nil {
				return planningObjectError("assessment requires exclusive ownership and both model representations")
			}
			seen[kind] = true
		}
	}
	return nil
}

func validatePlanningObjects(description *protocol.Description, engine *protocol.Engine, service *protocol.ModelPlanning, request *protocol.PlanFeaturesRequest) error {
	negotiated := EngineOffers(description, engine.GetId(), PlanningObjectContextFeature) && slices.Contains(request.GetTargetFeatures(), PlanningObjectContextFeature)
	if !negotiated {
		if request.GetDesiredObjects() != nil || request.GetCurrentObjects() != nil || len(request.GetContextKinds())+len(request.GetChangedSubjects())+len(service.GetContextKinds()) != 0 {
			return planningObjectError("object context was not negotiated")
		}
		return nil
	}
	if !slices.Equal(request.GetContextKinds(), service.GetContextKinds()) {
		return planningObjectError("assessment assignment changed")
	}
	for _, state := range []struct {
		value          *protocol.ComparisonState
		representation protocol.ValueInput_Representation
	}{{request.GetDesiredObjects(), protocol.ValueInput_DESIRED}, {request.GetCurrentObjects(), protocol.ValueInput_OBSERVED}} {
		if err := validatePlanningObjectState(description, engine, request, state.value, state.representation); err != nil {
			return err
		}
	}
	if err := validateComparisonIdentities(request.GetChangedSubjects()); err != nil {
		return err
	}
	if !slices.IsSortedFunc(request.GetChangedSubjects(), CompareIdentities) {
		return planningObjectError("changed subjects are not ordered")
	}
	all := planningObjectSubjects(request, nil)
	for _, subject := range request.GetChangedSubjects() {
		if !slices.ContainsFunc(all, func(input *protocol.ComparisonIdentity) bool { return proto.Equal(input, subject) }) {
			return planningObjectError("changed subject is outside the captured native inventory")
		}
	}
	return nil
}

func validatePlanningObjectState(description *protocol.Description, engine *protocol.Engine, request *protocol.PlanFeaturesRequest, state *protocol.ComparisonState, representation protocol.ValueInput_Representation) error {
	comparison := planningValueContext(request)
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == representation {
			comparison.Kinds = append(comparison.Kinds, model.GetKind())
		}
	}
	if err := validateComparisonState(description, comparison, &protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}, representation, state); err != nil {
		return err
	}
	for _, record := range state.GetRecords() {
		if record.GetKind() != record.GetSubject().GetKind() || !planningObjectIdentityMatches(request, record) {
			return planningObjectError("record does not bind its native model and exact owner")
		}
	}
	for _, covered := range state.GetCoverage().GetSubjects() {
		if covered.GetKind() != covered.GetSubject().GetKind() {
			return planningObjectError("coverage subject does not name its native model")
		}
	}
	return nil
}

func planningObjectIdentityMatches(request *protocol.PlanFeaturesRequest, record *protocol.ComparisonRecord) bool {
	if context := record.GetValue().GetValue().GetContext(); context != nil && context.GetDatabase() != request.GetDatabasePath() {
		return false
	}
	bound := record
	if record.GetSubject().GetDatabase() == nil && record.GetValue().GetValue().GetOwner().GetDatabase() != "" {
		// Inspection localizes the selected subject only after verifying both
		// captured spellings against the session database. Preserve that proof.
		if request.GetDatabasePath() == "" || record.GetValue().GetValue().GetOwner().GetDatabase() != request.GetDatabasePath() ||
			record.GetValue().GetNormalizedOwner().GetDatabase() != request.GetDatabasePath() {
			return false
		}
		bound = proto.CloneOf(record)
		bound.Value.Value.Owner.Database, bound.Value.NormalizedOwner.Database = "", ""
	}
	return declarationIdentityMatches(&protocol.PlanDeclarationsRequest{Engine: request.GetEngine(), Identifiers: request.GetIdentifiers()}, bound)
}

// PlanningObjectSubjects returns the exact ordered assessment inventory of a
// validated request. It includes records and subject coverage for assigned
// context kinds, pairs normalized identities, and prefers desired spelling.
// Returned subjects share no writable state with the request. Kind-wide unknown
// coverage has no invented subject and remains in the request for assessment.
func PlanningObjectSubjects(request *protocol.PlanFeaturesRequest) []*protocol.ComparisonIdentity {
	if len(request.GetContextKinds()) == 0 {
		return nil
	}
	return planningObjectSubjects(request, request.GetContextKinds())
}

func planningObjectSubjects(request *protocol.PlanFeaturesRequest, kinds []string) []*protocol.ComparisonIdentity {
	var subjects []*protocol.ComparisonIdentity
	add := func(kind string, subject *protocol.ComparisonIdentity) {
		if kinds != nil && !slices.Contains(kinds, kind) {
			return
		}
		subjects = append(subjects, proto.CloneOf(subject))
	}
	for _, state := range []*protocol.ComparisonState{request.GetDesiredObjects(), request.GetCurrentObjects()} {
		for _, record := range state.GetRecords() {
			add(record.GetKind(), record.GetSubject())
		}
		for _, covered := range state.GetCoverage().GetSubjects() {
			add(covered.GetKind(), covered.GetSubject())
		}
	}
	// Stable sorting keeps desired spelling before current spelling and a
	// captured record before its same-side coverage entry.
	slices.SortStableFunc(subjects, CompareIdentities)
	return slices.CompactFunc(subjects, func(a, b *protocol.ComparisonIdentity) bool { return CompareIdentities(a, b) == 0 })
}

func validatePlanningObjectReceipts(request *protocol.PlanFeaturesRequest, result *protocol.PlannedFeatures, steps map[planningStepKey]bool) error {
	inputs := PlanningObjectSubjects(request)
	if len(inputs) != len(result.GetObjects()) {
		return planningObjectError("assessment receipts do not cover the exact native inventory")
	}
	for i, subject := range inputs {
		if err := validatePlanningReceipt(result.Objects[i], subject, subject.GetKind(), steps); err != nil {
			return err
		}
	}
	return nil
}
