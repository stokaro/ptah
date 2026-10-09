package schemapreparation

import (
	"fmt"
	"slices"

	"ptah.run/core/internal/capturefacets"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func acceptResolvedFacets(source Table, records []schemaext.FacetRecord, semantics identifier.Semantics) error {
	if len(source.ResolvedFacets) != 0 {
		return fmt.Errorf("%w: preparation input already carries resolved facets", ErrInvalid)
	}
	declared, err := capturefacets.Declared(source.Desired, source.Subject, semantics)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range records {
		original, found := declared[record.Subject.Key()]
		if !found || original.Subject != record.Subject || seen[record.Subject.Key()] || record.Values.IsZero() {
			return fmt.Errorf("%w: missing, duplicate, or empty resolved facet owner %s", ErrInvalid, record.Subject)
		}
		seen[record.Subject.Key()] = true
		for _, kind := range record.Values.DeclaredKinds() {
			if !slices.Contains(original.Values.Kinds(), kind) || len(record.Values.TargetScope(kind)) != 0 {
				return fmt.Errorf("%w: resolved facets must retain declared kinds and their source scopes", ErrInvalid)
			}
		}
	}
	return nil
}
