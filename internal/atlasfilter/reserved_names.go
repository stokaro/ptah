package atlasfilter

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

// Reserved storage is already outside the managed description. An exclusion
// can name it, but cannot turn its occupied name into an available one.
func (s *exclusionState) noteReservedNames(names []catalog.ReservedName) {
	for _, record := range names {
		ref := record.Identity
		if ref.Kind == objectidentity.KindTable {
			s.matches("table", s.nameCandidates(ref.Schema.Authored(), ref.Name.Source)...)
			continue
		}
		for _, selection := range s.vocabulary.selections.Selections() {
			if objectidentity.Kind(selection.Kind) != ref.Kind {
				continue
			}
			if selection.ByNameAlone(ref.Schema.Authored()) {
				s.matches(selection.Type, ref.Name.Source)
			} else {
				s.matches(selection.Type, s.nameCandidates(ref.Schema.Authored(), ref.Name.Source)...)
			}
		}
	}
}
