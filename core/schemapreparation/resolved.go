package schemapreparation

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func acceptResolvedFacets(source Table, records []schemaext.FacetRecord, semantics identifier.Semantics) error {
	if len(source.ResolvedFacets) != 0 {
		return fmt.Errorf("%w: preparation input already carries resolved facets", ErrInvalid)
	}
	declared := map[objectidentity.Key]schemaext.FacetRecord{
		source.Subject.Key(): {Subject: source.Subject, Values: source.Desired.Table.Facets},
	}
	for _, index := range source.Desired.Indexes {
		subject := objectidentity.NewBuilder(semantics).IndexParts(source.Subject.Schema.Source, source.Subject.Name.Source, index.Name)
		if _, duplicate := declared[subject.Key()]; duplicate {
			return fmt.Errorf("%w: duplicate declared facet owner %s", ErrInvalid, subject)
		}
		declared[subject.Key()] = schemaext.FacetRecord{Subject: subject, Values: index.Facets}
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
