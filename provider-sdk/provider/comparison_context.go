package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateComparisonContext(request *protocol.CompareModelsRequest, service *protocol.ModelComparison) error {
	if err := ValidateIdentifierRules(request.GetIdentifiers().GetRules()); err != nil {
		return err
	}
	resolved := make(map[string]bool)
	for _, name := range request.GetIdentifiers().GetResolvedNames() {
		if name.GetName() == "" || name.GetKey() == "" || resolved[name.GetName()] {
			return fmt.Errorf("%w: invalid resolved comparison name", ErrProtocol)
		}
		resolved[name.GetName()] = true
	}
	for name := range request.GetCapabilities() {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: empty comparison capability", ErrProtocol)
		}
	}
	parents := make([]*protocol.ComparisonIdentity, 0, len(request.GetParents()))
	for _, parent := range request.GetParents() {
		parents = append(parents, parent.GetSubject())
		if !parent.GetDesired() && !parent.GetCurrent() {
			return fmt.Errorf("%w: comparison parent has no lifecycle", ErrProtocol)
		}
		if service.GetPlacement() == protocol.ModelComparison_FACETS && !slices.Contains(service.GetOwnerKinds(), parent.GetSubject().GetKind()) {
			return fmt.Errorf("%w: comparison parent kind is not owned", ErrProtocol)
		}
	}
	for _, refs := range [][]*protocol.ComparisonIdentity{parents, request.GetDeclaredRelations(), request.GetDeclaredRoles(), request.GetRequired()} {
		if err := validateComparisonIdentities(refs); err != nil {
			return err
		}
	}
	if err := validateComparisonDefinitions(request, service); err != nil {
		return err
	}
	return validateComparisonContextPlacement(request, service)
}

func validateComparisonDefinitions(request *protocol.CompareModelsRequest, service *protocol.ModelComparison) error {
	for _, change := range request.GetRequests() {
		if change.GetWritten() != "" {
			// The owner resolves the operator's spelling; the host sends no
			// subject beside it.
			if change.GetSubject() != nil || !diagnosticLine(change.GetWritten()) {
				return fmt.Errorf("%w: a written request carries one line of text and no subject", ErrProtocol)
			}
		} else if err := ValidateComparisonIdentity(change.GetSubject()); err != nil {
			return err
		}
		if !slices.Contains(service.GetActions(), change.GetAction()) {
			return fmt.Errorf("%w: comparison action is not owned", ErrProtocol)
		}
	}
	for _, column := range request.GetDeclaredColumns() {
		if err := ValidateComparisonIdentity(column.GetTable()); err != nil {
			return err
		}
		if column.GetName() == "" || column.GetType() == "" {
			return fmt.Errorf("%w: comparison column requires a name and type", ErrProtocol)
		}
	}
	for _, constraints := range [][]*protocol.ComparisonConstraint{request.GetDesiredConstraints(), request.GetCurrentConstraints()} {
		for _, constraint := range constraints {
			if err := ValidateComparisonIdentity(constraint.GetSubject()); err != nil {
				return err
			}
			if constraint.GetType() == "" {
				return fmt.Errorf("%w: comparison constraint requires a type", ErrProtocol)
			}
			if err := validateComparisonNames(constraint.GetColumns()); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateComparisonContextPlacement(request *protocol.CompareModelsRequest, service *protocol.ModelComparison) error {
	if service.GetPlacement() == protocol.ModelComparison_FACETS && (len(request.GetRequests()) != 0 || len(request.GetDeclaredRelations()) != 0 || len(request.GetDeclaredRoles()) != 0 ||
		request.GetDatabasePath() != "" || len(request.GetExclusions()) != 0 || len(request.GetDeclaredColumns()) != 0 || len(request.GetRequired()) != 0) {
		return fmt.Errorf("%w: facet comparison contains native object context", ErrProtocol)
	}
	if service.GetPlacement() == protocol.ModelComparison_OBJECTS && (len(request.GetDesiredConstraints()) != 0 || len(request.GetCurrentConstraints()) != 0) {
		return fmt.Errorf("%w: object comparison contains facet constraint context", ErrProtocol)
	}
	return nil
}
