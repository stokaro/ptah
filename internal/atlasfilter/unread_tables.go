package atlasfilter

import (
	"ptah.run/catalog"
	"ptah.run/internal/tableref"
)

// An unread table is already outside the description, as reserved storage
// is. An exclusion can name it, but cannot make the database lose it, so the
// filter keeps the record and only notes the match.
func (s *exclusionState) noteUnreadTables(tables []catalog.UnreadTable) {
	for _, table := range tables {
		s.matches("table", s.nameCandidates(table.Identity.Schema.Authored(), table.Identity.Name.Source)...)
	}
}

// A table an earlier filter took out is outside the description as well, and
// the database still holds it: an exclusion that names it again, as the second
// pass of a positive selection does, is not reported as matching nothing.
func (s *exclusionState) noteTablesOutOfScope(tables []string) {
	for _, table := range tables {
		if ref, ok := tableref.Parse(table); ok {
			s.matches("table", s.nameCandidates(ref.Schema, ref.Name)...)
		}
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

// An object of a kind Ptah does not model is outside the description, and a
// report names it so that nobody reads the report as covering it. A selection
// that leaves the object out says the run is not about it, so the report does
// not name it either: the object leaves when a selector names it or the schema
// it names, and a property of a table leaves with the table. A record of every
// object of a kind names no object, so only its schema can take it out. No
// schema selector takes out an object that names no schema: a read of one
// MySQL database names none, since the database is the read's, and a
// PostgreSQL role setting is in no schema. A selector that names an object
// here is matched, so it is not reported as matching nothing.
func (s *exclusionState) filterUnmodeledObjects(objects []catalog.UnmodeledObject) []catalog.UnmodeledObject {
	return keep(objects, func(object catalog.UnmodeledObject) bool {
		if object.Table != "" {
			return !s.tableExcluded(object.Schema, object.Table)
		}
		if object.Schema != "" && s.schemaExcluded(object.Schema) {
			return false
		}
		return object.Name == "" || !s.matches(object.Kind, s.nameCandidates(object.Schema, object.Name)...)
	})
}
