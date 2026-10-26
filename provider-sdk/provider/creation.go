package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ProjectTableCreationsFeature advertises target-owned offline CREATE prediction.
const ProjectTableCreationsFeature = "schema.project-table-creations"

// TableCreationProjector computes creation effects without opening a session.
// Predictions retain declarations and cannot establish catalog inspection.
type TableCreationProjector interface {
	ProjectTableCreations(context.Context, *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error)
}

// Preparation and CREATE prediction adjust the same common facet owner kinds.
// The request's captured owner inventory further bounds CREATE adjustments.
var tableFacetOwnerKinds = []string{"table", "index", "primary key", "constraint"}

// ValidateCreationRequest checks declarations, target facts, and the caller's
// explicit set of common owners eligible for computed creation effects.
func ValidateCreationRequest(description *protocol.Description, request *protocol.ProjectTableCreationsRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ProjectTableCreationsFeature) {
		return fmt.Errorf("%w: creation projection is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.Engine, request.TargetFeatures)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ProjectTableCreationsFeature) {
		return fmt.Errorf("%w: engine creation projection is unavailable", ErrProtocol)
	}
	captured := creationContext(request)
	if err := validatePlanningContext(description, captured); err != nil {
		return err
	}
	for _, table := range request.Tables {
		if table.GetDeclaration() == nil {
			return fmt.Errorf("%w: creation projection requires a declaration", ErrProtocol)
		}
		if err := validatePreparationIdentity(table.Subject, table.Declaration.Name, table.Declaration.Schema, request.Identifiers.GetDefaultSchema()); err != nil {
			return err
		}
		if err := validateCreationOwners(table); err != nil {
			return err
		}
	}
	return validatePlanningTables(description, captured, engine)
}

func creationContext(request *protocol.ProjectTableCreationsRequest) *protocol.PlanFeaturesRequest {
	result := &protocol.PlanFeaturesRequest{Engine: request.GetEngine(), Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities(), TargetFeatures: request.GetTargetFeatures()}
	for _, table := range request.GetTables() {
		result.Tables = append(result.Tables, &protocol.PlanningTable{Subject: table.GetSubject(), Desired: table.GetDeclaration(), Action: protocol.PlanningTable_CREATE})
	}
	return result
}

func validateCreationOwners(table *protocol.TableCreationInput) error {
	if err := validateComparisonIdentities(table.FacetOwners); err != nil {
		return err
	}
	if !hasCreationOwner(table, table.Subject) {
		return fmt.Errorf("%w: creation owner inventory omits its table", ErrProtocol)
	}
	for _, owner := range table.FacetOwners {
		if !slices.Contains(tableFacetOwnerKinds, owner.Kind) || (owner.Kind == "table" && !proto.Equal(owner, table.Subject)) {
			return fmt.Errorf("%w: invalid creation facet owner kind", ErrProtocol)
		}
	}
	for _, record := range table.Declaration.GetFeatures().GetFacets().GetRecords() {
		if slices.Contains(tableFacetOwnerKinds, record.GetSubject().GetKind()) && !hasCreationOwner(table, record.Subject) {
			return fmt.Errorf("%w: creation owner inventory omits a declared facet", ErrProtocol)
		}
	}
	return nil
}

func hasCreationOwner(table *protocol.TableCreationInput, owner *protocol.ComparisonIdentity) bool {
	return slices.ContainsFunc(table.FacetOwners, func(captured *protocol.ComparisonIdentity) bool { return proto.Equal(captured, owner) })
}

// ValidateProjectedCreations refuses partial, reordered, or unbound predictions.
// Computed facets may add kinds, but cannot restore exclusions or change bindings.
func ValidateProjectedCreations(description *protocol.Description, request *protocol.ProjectTableCreationsRequest, result *protocol.ProjectedTableCreations) error {
	if err := ValidateCreationRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.Complete || len(result.Tables) != len(request.Tables) {
		return fmt.Errorf("%w: incomplete creation projection", ErrProtocol)
	}
	engine, _, _ := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	for i, output := range result.Tables {
		input := request.Tables[i]
		if !proto.Equal(input.Subject, output.GetSubject()) {
			return fmt.Errorf("%w: creation projection changed table identity or order", ErrProtocol)
		}
		if err := validateCreationColumns(engine, input.Declaration, output); err != nil {
			return err
		}
		if err := validateColumnKeys(input.Declaration, output); err != nil {
			return err
		}
		if err := validateCreationRecords(description, request, input, protocol.ValueInput_DESIRED, output.DesiredFacets); err != nil {
			return err
		}
		if err := validateCreationRecords(description, request, input, protocol.ValueInput_OBSERVED, output.ObservedFacets); err != nil {
			return err
		}
	}
	return nil
}
