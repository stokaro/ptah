package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ReverseChangesFeature enables ReverseChanges and Engine.reversals: the
// owner of a change kind undoes an accepted forward change of that kind, so
// the host can write the reverse plan of a migration whose changes it cannot
// read. The provider and the engine list it, and both sides list it to use
// it. It requires PlanFeaturesFeature, because a reversed change is planned
// as any other change is, and CompareModelsFeature, which produces the
// changes a reversal receives.
const ReverseChangesFeature = "models.reverse"

// ChangeReverser undoes accepted changes offline.
type ChangeReverser interface {
	// ReverseChanges returns exactly one result per change of the request, in
	// order: the change that undoes it, what the forward change leaves at its
	// subject, and what the reverse cannot recover. It reads no database and
	// takes nothing from a source document. A change it cannot undo fails the
	// whole call.
	ReverseChanges(context.Context, *protocol.ReverseChangesRequest) (*protocol.ReversedChanges, error)
}

func reversalError(reason string) error {
	return fmt.Errorf("%w: change reversal: %s", ErrProtocol, reason)
}

// validateModelReversals checks the reversal services an engine declares:
// the feature listed by the provider and the engine beside planning and
// comparison, a unique service name each, and each change kind a CHANGE model
// of the engine that one service alone owns.
func validateModelReversals(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ReverseChangesFeature)
	if !listed {
		if len(engine.GetReversals()) != 0 {
			return reversalError("the engine declares reversal services without the feature")
		}
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ReverseChangesFeature) || !slices.Contains(description.GetFeatures(), PlanFeaturesFeature) ||
		!slices.Contains(description.GetFeatures(), CompareModelsFeature) {
		return reversalError("the engine requires the provider's feature, planning and comparison")
	}
	if len(engine.GetReversals()) == 0 {
		return reversalError("the engine lists the feature and declares no reversal service")
	}
	services, kinds := make(map[string]bool), make(map[string]bool)
	for _, service := range engine.GetReversals() {
		if !validName(service.GetId()) || services[service.GetId()] || len(service.GetChangeKinds()) == 0 {
			return reversalError("a reversal service is unnamed, repeated or owns no change kind")
		}
		services[service.GetId()] = true
		if err := validateComparisonNames(service.GetChangeKinds()); err != nil {
			return err
		}
		for _, kind := range service.GetChangeKinds() {
			if kinds[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_CHANGE) == nil {
				return reversalError("a change kind has no CHANGE model or two reversal services")
			}
			kinds[kind] = true
		}
	}
	return nil
}

func findModelReversal(engine *protocol.Engine, id string) *protocol.ModelReversal {
	for _, service := range engine.GetReversals() {
		if service.GetId() == id {
			return service
		}
	}
	return nil
}

// ValidateReverseChangesRequest refuses a request the negotiated description
// does not list ReverseChangesFeature for, one for an engine or a service that
// does not declare it, and one whose changes are not valid CHANGE values of
// the service's kinds, each subject and kind once.
func ValidateReverseChangesRequest(description *protocol.Description, request *protocol.ReverseChangesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ReverseChangesFeature) {
		return reversalError("the feature was not negotiated")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelReversals(description, engine); err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ReverseChangesFeature) || !slices.Contains(request.GetTargetFeatures(), ReverseChangesFeature) {
		return reversalError("the engine does not reverse changes")
	}
	service := findModelReversal(engine, request.GetReversalId())
	if service == nil {
		return reversalError("the reversal service is unavailable")
	}
	if request.GetIdentifiers() == nil {
		return reversalError("the request carries no identifier rules")
	}
	if err := ValidateIdentifierRules(request.GetIdentifiers().GetRules()); err != nil {
		return err
	}
	if len(request.GetChanges()) == 0 {
		return reversalError("the request carries no change")
	}
	values := make([]*protocol.NormalizedValue, 0, len(request.GetChanges()))
	records := make([]*protocol.ComparisonRecord, 0, len(request.GetChanges()))
	for _, change := range request.GetChanges() {
		kind := change.GetValue().GetValue().GetKind()
		if !slices.Contains(service.GetChangeKinds(), kind) {
			return reversalError("a change kind belongs to another service")
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
			return reversalError("a subject and change kind appear twice")
		}
	}
	return validateComparisonValues(description, reversalValueContext(request), protocol.ValueInput_CHANGE, values)
}

func reversalValueContext(request *protocol.ReverseChangesRequest) *protocol.CompareModelsRequest {
	return &protocol.CompareModelsRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}
}

// ValidateReversedChanges checks a reply against its request: complete, one
// result per change in order, each keeping its change's subject, with a
// one-line strategy and one-line limitations. A present value is a valid
// CHANGE value of the forward change's kind; an absent one comes with a
// limitation. Each predicted forward state names a placement and an OBSERVED
// model of the engine, an object state the subject's own kind, and each
// subject, placement and kind once across the batch; a present value is a
// valid OBSERVED value of that kind.
func ValidateReversedChanges(description *protocol.Description, request *protocol.ReverseChangesRequest, result *protocol.ReversedChanges) error {
	if err := ValidateReverseChangesRequest(description, request); err != nil {
		return err
	}
	return ValidateReversedChangesReply(description, request, result)
}

// ValidateReversedChangesReply is [ValidateReversedChanges] for a request that
// [ValidateReverseChangesRequest] accepted with description and that has not
// changed since: it checks the reply and not the request again. The SDK's
// client and server validate each request once and call it for the reply.
func ValidateReversedChangesReply(description *protocol.Description, request *protocol.ReverseChangesRequest, result *protocol.ReversedChanges) error {
	if result == nil || !result.GetComplete() || len(result.GetChanges()) != len(request.GetChanges()) {
		return reversalError("the reply is incomplete")
	}
	var changes, observed []*protocol.NormalizedValue
	var states []forwardStateKey
	for i, reversed := range result.GetChanges() {
		input := request.GetChanges()[i]
		if err := validateReversedChange(input, reversed); err != nil {
			return err
		}
		if value := reversed.GetValue(); value != nil {
			changes = append(changes, value)
		}
		for _, state := range reversed.GetForwardState() {
			if err := validateForwardState(description, request.GetEngine(), input.GetSubject(), state); err != nil {
				return err
			}
			states = append(states, forwardStateKey{input.GetSubject(), state.GetPlacement(), state.GetKind()})
			if value := state.GetValue(); value != nil {
				observed = append(observed, value)
			}
		}
	}
	slices.SortFunc(states, compareForwardStates)
	for i := 1; i < len(states); i++ {
		if compareForwardStates(states[i-1], states[i]) == 0 {
			return reversalError("a subject's forward state names one placement and kind twice")
		}
	}
	if err := validateComparisonValues(description, reversalValueContext(request), protocol.ValueInput_CHANGE, changes); err != nil {
		return err
	}
	return validateComparisonValues(description, reversalValueContext(request), protocol.ValueInput_OBSERVED, observed)
}

// validateReversedChange checks one result against the change it undoes.
func validateReversedChange(input *protocol.PlanningChange, reversed *protocol.ReversedChange) error {
	if CompareIdentities(reversed.GetSubject(), input.GetSubject()) != 0 {
		return reversalError("a result changed its subject")
	}
	if !diagnosticLine(reversed.GetStrategy()) || slices.ContainsFunc(reversed.GetLimitations(), func(limitation string) bool { return !diagnosticLine(limitation) }) {
		return reversalError("a strategy or a limitation is not one line")
	}
	value := reversed.GetValue()
	if value == nil && len(reversed.GetLimitations()) == 0 {
		return reversalError("a reverse without a change states no limitation")
	}
	if value != nil && value.GetValue().GetKind() != input.GetValue().GetValue().GetKind() {
		return reversalError("a result changed the change kind")
	}
	return nil
}

// validateForwardState checks one predicted state of subject.
func validateForwardState(description *protocol.Description, engine string, subject *protocol.ComparisonIdentity, state *protocol.ForwardState) error {
	placement, kind := state.GetPlacement(), state.GetKind()
	if placement != protocol.ForwardState_OBJECT && placement != protocol.ForwardState_FACET {
		return reversalError("a forward state has no placement")
	}
	if placement == protocol.ForwardState_OBJECT && kind != subject.GetKind() {
		return reversalError("an object state names another kind than its subject")
	}
	if findValueModel(description, engine, kind, protocol.ValueInput_OBSERVED) == nil {
		return reversalError("a forward state names a kind with no OBSERVED model")
	}
	if value := state.GetValue(); value != nil && value.GetValue().GetKind() != kind {
		return reversalError("a forward state value differs from its kind")
	}
	return nil
}

// forwardStateKey is what one predicted forward state stands for: one value
// of one kind at one placement of one subject.
type forwardStateKey struct {
	subject   *protocol.ComparisonIdentity
	placement protocol.ForwardState_Placement
	kind      string
}

func compareForwardStates(a, b forwardStateKey) int {
	if compared := CompareIdentities(a.subject, b.subject); compared != 0 {
		return compared
	}
	if a.placement != b.placement {
		return int(a.placement) - int(b.placement)
	}
	return strings.Compare(a.kind, b.kind)
}
