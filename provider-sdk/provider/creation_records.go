package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validateCreationRecords(description *protocol.Description, request *protocol.ProjectTableCreationsRequest, input *protocol.TableCreationInput,
	representation protocol.ValueInput_Representation, records []*protocol.ComparisonRecord,
) error {
	engine, _, err := valueTarget(description, request.Engine, request.TargetFeatures)
	if err != nil {
		return err
	}
	values := planningValueContext(creationContext(request))
	for _, model := range engine.ValueModels {
		if model.Representation == representation {
			values.Kinds = append(values.Kinds, model.Kind)
		}
	}
	for _, record := range records {
		if record.GetValue() == nil || len(record.GetTargets()) != 0 || !hasCreationOwner(input, record.GetSubject()) {
			return fmt.Errorf("%w: creation facets require values on captured owners without bindings", ErrProtocol)
		}
		if err := retainCreationFacetIdentity(input, record); err != nil {
			return err
		}
		if representation == protocol.ValueInput_OBSERVED && (!slices.Contains(description.Features, ConvertValuesFeature) ||
			findValueModel(description, request.Engine, record.Kind, protocol.ValueInput_DESIRED) == nil) {
			return fmt.Errorf("%w: predicted observation has no target conversion", ErrProtocol)
		}
	}
	state := &protocol.ComparisonState{Representation: representation, Coverage: &protocol.ComparisonCoverage{}, Records: records}
	service := &protocol.ModelComparison{Placement: protocol.ModelComparison_FACETS, OwnerKinds: tableFacetOwnerKinds}
	return validateComparisonState(description, values, service, representation, state)
}

func retainCreationFacetIdentity(input *protocol.TableCreationInput, record *protocol.ComparisonRecord) error {
	for _, original := range input.Declaration.GetFeatures().GetFacets().GetRecords() {
		if original.Kind != record.Kind || !proto.Equal(original.Subject, record.Subject) {
			continue
		}
		if original.Value == nil {
			return fmt.Errorf("%w: creation projection restored an excluded facet", ErrProtocol)
		}
		if !proto.Equal(original.Value.GetValue().GetOwner(), record.Value.GetValue().GetOwner()) ||
			compareReferences(original.Value.NormalizedOwner, record.Value.NormalizedOwner) != 0 {
			return fmt.Errorf("%w: creation projection changed a facet owner", ErrProtocol)
		}
	}
	return nil
}
