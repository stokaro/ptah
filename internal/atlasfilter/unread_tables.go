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

// An object that blocks a table's removal is outside the description too: an
// exclusion that names it keeps it, since the database still holds it, and is
// not reported as matching nothing. Spanner keeps a change stream in the
// table's database rather than in a schema of its own, so the object is named
// as it is or under its table's schema.
func (s *exclusionState) noteRemovalBlockers(blockers []catalog.RemovalBlocker) {
	for _, blocker := range blockers {
		s.matches(blocker.Kind, s.nameCandidates(blocker.Schema, blocker.Name)...)
	}
}

// An object of a kind Ptah does not model is outside the description for the same
// reason, so a selector that names one is not reported as matching nothing.
func (s *exclusionState) noteUnmodeledObjects(objects []catalog.UnmodeledObject) {
	for _, object := range objects {
		s.matches(object.Kind, s.nameCandidates(object.Schema, object.Name)...)
	}
}
