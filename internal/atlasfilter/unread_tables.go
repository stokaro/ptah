package atlasfilter

import "ptah.run/catalog"

// An unread table is already outside the description, as reserved storage
// is. An exclusion can name it, but cannot make the database lose it, so the
// filter keeps the record and only notes the match.
func (s *exclusionState) noteUnreadTables(tables []catalog.UnreadTable) {
	for _, table := range tables {
		s.matches("table", s.nameCandidates(table.Identity.Schema.Authored(), table.Identity.Name.Source)...)
	}
}

// An object of a kind Ptah does not model is outside the description for the same
// reason, so a selector that names one is not reported as matching nothing.
func (s *exclusionState) noteUnmodeledObjects(objects []catalog.UnmodeledObject) {
	for _, object := range objects {
		s.matches(object.Kind, s.nameCandidates(object.Schema, object.Name)...)
	}
}
