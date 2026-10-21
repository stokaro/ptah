package provider

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validatePlanningColumn(description *protocol.Description, request *protocol.PlanFeaturesRequest, column *protocol.PlanningColumn) error {
	if !comparisonReason(column.GetName()) || !comparisonReason(column.GetType()) {
		return fmt.Errorf("%w: planned column requires a name and type", ErrProtocol)
	}
	if err := validatePlanningFacets(description, request, column.GetFacets()); err != nil {
		return err
	}
	if foreign := column.GetForeignKey(); foreign != nil {
		if !comparisonReason(foreign.GetTable()) || foreign.GetColumn() == "" && len(foreign.GetColumns()) == 0 {
			return fmt.Errorf("%w: planned foreign key requires its referenced table and columns", ErrProtocol)
		}
		if err := validateComparisonNames(foreign.GetColumns()); err != nil {
			return err
		}
		if err := validatePlanningFacets(description, request, foreign.GetFacets()); err != nil {
			return err
		}
	}
	return nil
}

func validatePlanningFacets(description *protocol.Description, request *protocol.PlanFeaturesRequest, facets []*protocol.CapturedFacet) error {
	seen := make(map[string]bool)
	values := make([]*protocol.NormalizedValue, 0, len(facets))
	for _, facet := range facets {
		if seen[facet.GetKind()] || findValueModel(description, request.GetEngine(), facet.GetKind(), protocol.ValueInput_DESIRED) == nil {
			return fmt.Errorf("%w: duplicate or unowned planned operand facet", ErrProtocol)
		}
		seen[facet.GetKind()] = true
		if err := validateComparisonNames(facet.GetTargets()); err != nil {
			return err
		}
		if facet.GetValue() == nil {
			if len(facet.GetTargets()) == 0 {
				return fmt.Errorf("%w: planned facet exclusion requires its target scope", ErrProtocol)
			}
			continue
		}
		if facet.GetValue().GetValue().GetKind() != facet.GetKind() {
			return fmt.Errorf("%w: planned operand facet changed its model", ErrProtocol)
		}
		values = append(values, facet.GetValue())
	}
	return validateComparisonValues(description, planningValueContext(request), protocol.ValueInput_DESIRED, values)
}
