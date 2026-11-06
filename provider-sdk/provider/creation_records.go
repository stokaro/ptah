package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// creationRecordChecks checks the facets of the tables of one creation
// projection: checks holds the answers of the reply's validation, and
// contexts the comparison context of each representation.
type creationRecordChecks struct {
	checks   *targetChecks
	contexts map[protocol.ValueInput_Representation]*protocol.CompareModelsRequest
}

// validate checks the facets of one table of the reply for representation,
// in the context engine gives that representation.
func (c creationRecordChecks) validate(description *protocol.Description, request *protocol.ProjectTableCreationsRequest, engine *protocol.Engine,
	input *protocol.TableCreationInput, representation protocol.ValueInput_Representation, records []*protocol.ComparisonRecord,
) error {
	values, built := c.contexts[representation]
	if !built {
		values = &protocol.CompareModelsRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Identifiers: request.GetIdentifiers()}
		for _, model := range engine.ValueModels {
			if model.Representation == representation {
				values.Kinds = append(values.Kinds, model.Kind)
			}
		}
		c.contexts[representation] = values
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
	return checkComparisonState(c.checks, description, values, service, representation, state)
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
