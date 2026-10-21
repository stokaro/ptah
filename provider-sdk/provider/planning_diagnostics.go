package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validatePlanningDiagnostics(request *protocol.PlanFeaturesRequest, result *protocol.PlannedFeatures) error {
	if len(result.GetContributions())+len(result.GetChanges())+len(result.GetParents())+len(result.GetRewrites()) != 0 {
		return fmt.Errorf("%w: refused planning batch contains successful output", ErrProtocol)
	}
	for _, diagnostic := range result.GetDiagnostics() {
		switch diagnostic.GetCode() {
		case protocol.PlanningDiagnostic_INVALID_SCHEMA, protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, protocol.PlanningDiagnostic_OMITTED_DECLARATION:
		default:
			return fmt.Errorf("%w: unknown planning diagnostic code", ErrProtocol)
		}
		if !comparisonReason(diagnostic.GetKind()) || !comparisonReason(diagnostic.GetMessage()) {
			return fmt.Errorf("%w: planning diagnostic requires a kind and message", ErrProtocol)
		}
		switch index := diagnostic.GetInput().(type) {
		case *protocol.PlanningDiagnostic_Change:
			if index == nil || uint64(index.Change) >= uint64(len(request.GetChanges())) {
				return fmt.Errorf("%w: planning diagnostic change is outside the batch", ErrProtocol)
			}
			if diagnostic.GetKind() != request.GetChanges()[index.Change].GetValue().GetValue().GetKind() {
				return fmt.Errorf("%w: planning diagnostic change kind differs", ErrProtocol)
			}
		case *protocol.PlanningDiagnostic_Parent:
			if index == nil || uint64(index.Parent) >= uint64(len(request.GetTables())) || request.GetTables()[index.Parent].GetAction() == protocol.PlanningTable_CONTEXT {
				return fmt.Errorf("%w: planning diagnostic parent has no action", ErrProtocol)
			}
			if !slices.Contains(request.GetParentKinds(), diagnostic.GetKind()) {
				return fmt.Errorf("%w: planning diagnostic parent kind is unassigned", ErrProtocol)
			}
		case nil:
		default:
			return fmt.Errorf("%w: unknown planning diagnostic input", ErrProtocol)
		}
	}
	return nil
}
