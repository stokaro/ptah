package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// PlanningPolicyFeature carries the operator's planning policy to the
// provider: PlanFeaturesRequest.policy, and requests the owner resolves from
// the operator's spelling, RequestedChange.written with
// ComparedModels.resolved_requests. It is a two-way feature. A provider lists
// it, and so does each engine that honors it. Contract 25.3.
const PlanningPolicyFeature = "planning.policy"

// PlanningStatementStyleFeature proves that both sides honor the selected
// statement style in feature-owner operations. Contract 25.8.
const PlanningStatementStyleFeature = "planning.statement-style"

// planningPolicyNegotiated reports whether both sides listed the feature for
// an engine.
func planningPolicyNegotiated(description *protocol.Description, engine *protocol.Engine) bool {
	return slices.Contains(description.GetFeatures(), PlanningPolicyFeature) && slices.Contains(engine.GetFeatures(), PlanningPolicyFeature)
}

// validatePlanningPolicy checks a policy before owner code runs: negotiated,
// naming how the operator asks for a rebuild, and naming each index once.
func validatePlanningPolicy(description *protocol.Description, engine *protocol.Engine, policy *protocol.PlanningPolicy) error {
	if policy == nil {
		return nil
	}
	if !planningPolicyNegotiated(description, engine) {
		return fmt.Errorf("%w: %s was not negotiated", ErrProtocol, PlanningPolicyFeature)
	}
	switch policy.GetStatementStyle() {
	case protocol.PlanningStatementStyle_PLANNING_STATEMENT_STYLE_ANNOTATED:
	case protocol.PlanningStatementStyle_PLANNING_STATEMENT_STYLE_EXACT:
		if !slices.Contains(description.GetFeatures(), PlanningStatementStyleFeature) ||
			!slices.Contains(engine.GetFeatures(), PlanningStatementStyleFeature) {
			return fmt.Errorf("%w: exact planning needs %s", ErrProtocol, PlanningStatementStyleFeature)
		}
	default:
		return fmt.Errorf("%w: unknown planning statement style %d", ErrProtocol, policy.GetStatementStyle())
	}
	if !diagnosticLine(policy.GetTableRebuildRequest()) {
		return fmt.Errorf("%w: a planning policy names how the operator asks for a table rebuild", ErrProtocol)
	}
	for _, subjects := range [][]*protocol.ComparisonIdentity{policy.GetConcurrentIndexBuildSubjects(), policy.GetConcurrentIndexDropSubjects()} {
		for i, subject := range subjects {
			if err := ValidateComparisonIdentity(subject); err != nil {
				return err
			}
			if subject.GetKind() != "index" || slices.ContainsFunc(subjects[:i], func(other *protocol.ComparisonIdentity) bool { return CompareIdentities(other, subject) == 0 }) {
				return fmt.Errorf("%w: a planning policy names an index once", ErrProtocol)
			}
		}
	}
	return nil
}

// validateWrittenRequests refuses a written request a host sends without the
// negotiated feature.
func validateWrittenRequests(description *protocol.Description, engine *protocol.Engine, request *protocol.CompareModelsRequest) error {
	if planningPolicyNegotiated(description, engine) {
		return nil
	}
	for _, change := range request.GetRequests() {
		if change.GetWritten() != "" {
			return fmt.Errorf("%w: a written request needs %s", ErrProtocol, PlanningPolicyFeature)
		}
	}
	return nil
}

// validateResolvedRequests checks the requests a comparison service says it
// recognized: each a written request of the batch, answered once, with a
// desired object of a kind the service compares.
func validateResolvedRequests(description *protocol.Description, engine *protocol.Engine, request *protocol.CompareModelsRequest, result *protocol.ComparedModels) error {
	resolved := result.GetResolvedRequests()
	if len(resolved) == 0 {
		return nil
	}
	if !planningPolicyNegotiated(description, engine) {
		return fmt.Errorf("%w: resolved requests need %s", ErrProtocol, PlanningPolicyFeature)
	}
	answered := make(map[uint32]bool, len(resolved))
	for _, answer := range resolved {
		index := answer.GetRequest()
		if int(index) >= len(request.GetRequests()) || request.GetRequests()[index].GetWritten() == "" || answered[index] {
			return fmt.Errorf("%w: a resolved request names no written request of the batch, or one twice", ErrProtocol)
		}
		answered[index] = true
		subject := answer.GetSubject()
		if err := ValidateComparisonIdentity(subject); err != nil {
			return err
		}
		// Every desired record is of a model the service compares.
		desired := slices.ContainsFunc(request.GetDesired().GetRecords(), func(record *protocol.ComparisonRecord) bool {
			return CompareIdentities(record.GetSubject(), subject) == 0
		})
		if !desired {
			return fmt.Errorf("%w: a request resolves to a desired object the service compares", ErrProtocol)
		}
	}
	return nil
}
