package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateComparisonState(description *protocol.Description, request *protocol.CompareModelsRequest, service *protocol.ModelComparison, representation protocol.ValueInput_Representation, state *protocol.ComparisonState) error {
	if state == nil || state.GetRepresentation() != representation {
		return fmt.Errorf("%w: comparison state has the wrong representation", ErrProtocol)
	}
	values := make([]*protocol.NormalizedValue, 0, len(state.GetRecords()))
	for _, record := range state.GetRecords() {
		if !slices.Contains(request.GetKinds(), record.GetKind()) {
			return fmt.Errorf("%w: comparison record has an unowned model", ErrProtocol)
		}
		if err := ValidateComparisonIdentity(record.GetSubject()); err != nil {
			return err
		}
		if err := validateComparisonNames(record.GetTargets()); err != nil {
			return err
		}
		if service.GetPlacement() == protocol.ModelComparison_FACETS && !slices.Contains(service.GetOwnerKinds(), record.GetSubject().GetKind()) {
			return fmt.Errorf("%w: facet subject has an unowned kind", ErrProtocol)
		}
		if record.GetValue() == nil {
			if service.GetPlacement() != protocol.ModelComparison_FACETS || len(record.GetTargets()) == 0 {
				return fmt.Errorf("%w: missing value is not an explicitly excluded facet", ErrProtocol)
			}
			continue
		}
		if record.GetValue().GetValue().GetKind() != record.GetKind() {
			return fmt.Errorf("%w: comparison record differs from its value model", ErrProtocol)
		}
		values = append(values, record.GetValue())
	}
	ordered := slices.Clone(state.GetRecords())
	slices.SortFunc(ordered, compareComparisonRecords)
	for i := 1; i < len(ordered); i++ {
		if compareComparisonRecords(ordered[i-1], ordered[i]) == 0 {
			return fmt.Errorf("%w: duplicate comparison record", ErrProtocol)
		}
	}
	if err := validateComparisonValues(description, request, representation, values); err != nil {
		return err
	}
	return validateComparisonCoverage(description, request, representation, state.GetCoverage())
}

func compareComparisonRecords(a, b *protocol.ComparisonRecord) int {
	if compared := strings.Compare(a.GetKind(), b.GetKind()); compared != 0 {
		return compared
	}
	return CompareIdentities(a.GetSubject(), b.GetSubject())
}

func validateComparisonValues(description *protocol.Description, request *protocol.CompareModelsRequest, representation protocol.ValueInput_Representation, values []*protocol.NormalizedValue) error {
	inputs := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}
	for _, value := range values {
		inputs.Values = append(inputs.Values, &protocol.ValueInput{Id: value.GetId(), Representation: representation, Value: value.GetValue()})
	}
	return ValidateNormalizedValues(description, inputs, &protocol.NormalizedValues{Complete: true, Values: values})
}

func validateComparedChanges(description *protocol.Description, request *protocol.CompareModelsRequest, service *protocol.ModelComparison, changes []*protocol.ComparedChange) error {
	values := make([]*protocol.NormalizedValue, 0, len(changes))
	records := make([]*protocol.ComparisonRecord, 0, len(changes))
	for _, change := range changes {
		if !slices.Contains(request.GetKinds(), change.GetKind()) || !slices.Contains(service.GetChangeKinds(), change.GetValue().GetValue().GetKind()) {
			return fmt.Errorf("%w: comparison returned an unowned change model", ErrProtocol)
		}
		if err := ValidateComparisonIdentity(change.GetSubject()); err != nil {
			return err
		}
		if service.GetPlacement() == protocol.ModelComparison_FACETS && !slices.Contains(service.GetOwnerKinds(), change.GetSubject().GetKind()) {
			return fmt.Errorf("%w: facet change has an unowned subject kind", ErrProtocol)
		}
		records = append(records, &protocol.ComparisonRecord{Kind: change.GetKind(), Subject: change.GetSubject()})
		values = append(values, change.GetValue())
	}
	slices.SortFunc(records, compareComparisonRecords)
	for i := 1; i < len(records); i++ {
		if compareComparisonRecords(records[i-1], records[i]) == 0 {
			return fmt.Errorf("%w: duplicate comparison change", ErrProtocol)
		}
	}
	return validateComparisonValues(description, request, protocol.ValueInput_CHANGE, values)
}
