package engine

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func validateFacetOwnerKinds(kinds []objectidentity.Kind) error {
	if len(kinds) == 0 {
		return fmt.Errorf("%w: facet comparison requires common owner kinds", ErrInvalidRegistration)
	}
	seen := make(map[objectidentity.Kind]bool)
	for _, kind := range kinds {
		if kind == "" || strings.TrimSpace(string(kind)) != string(kind) || seen[kind] {
			return fmt.Errorf("%w: invalid or duplicate facet owner kind %q", ErrInvalidRegistration, kind)
		}
		seen[kind] = true
	}
	return nil
}

// Validate the complete input before any provider runs. Filtering an invalid
// attachment out of a batch would otherwise turn encountered state into a no-op.
func (r *Runtime) facetInputOwnerKinds(request schemaext.FacetComparisonRequest) error {
	for _, state := range []schemaext.FacetState{request.Desired, request.Current} {
		for _, record := range state.Records {
			for _, kind := range record.Values.Kinds() {
				if err := r.facetOwnerKind(request.Target, kind, record.Subject.Kind); err != nil {
					return err
				}
			}
		}
		for _, record := range state.Coverage.SubjectRecords() {
			if err := r.facetOwnerKind(request.Target, record.Kind, record.Subject.Kind); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runtime) facetOwnerKind(target string, kind schemaext.Kind, owner objectidentity.Kind) error {
	service, found := r.facetComparisons[conversionKey{target: target, kind: kind}]
	if found && !slices.Contains(r.facetServices[service].OwnerKinds, owner) {
		return fmt.Errorf("%w: facet %q does not attach to common owner kind %q on %q", schemaext.ErrInvalidValue, kind, owner, target)
	}
	return nil
}

func facetOwnersOfKinds(owners []schemaext.ParentState, kinds []objectidentity.Kind) []schemaext.ParentState {
	return slices.DeleteFunc(slices.Clone(owners), func(owner schemaext.ParentState) bool {
		return !slices.Contains(kinds, owner.Subject.Kind)
	})
}

func selectFacetOwners(request schemaext.FacetComparisonRequest, kinds []objectidentity.Kind) (schemaext.FacetComparisonRequest, schemaext.FacetState) {
	request.Owners = facetOwnersOfKinds(request.Owners, kinds)
	retained := schemaext.FacetState{}
	var records []schemaext.FacetRecord
	for _, record := range request.Desired.Records {
		if slices.Contains(kinds, record.Subject.Kind) {
			records = append(records, record)
		} else {
			// Input validation permits only excluded source bindings here.
			// Keep them for later targets without sending them to this service.
			retained.Records = append(retained.Records, record)
		}
	}
	request.Desired.Records = records
	request.Current.Records = slices.DeleteFunc(slices.Clone(request.Current.Records), func(record schemaext.FacetRecord) bool {
		return !slices.Contains(kinds, record.Subject.Kind)
	})
	return request, retained
}
