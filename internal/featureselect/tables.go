// Package featureselect keeps feature objects and knowledge aligned with common
// table selection, including the objects that bind tables without belonging to
// one, which their owners' relation discovery describes.
package featureselect

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Tables selects table-owned feature objects and their knowledge with the same
// parent selection. Filtering either alone loses retained state or leaves an
// unknown child attached to a table outside the comparison. Kind-wide knowledge
// remains the source's claim, including namespaces with no concrete values.
// Revision-table removal and user-selected scopes share this rule so neither
// leaves orphaned claims or drops the unknown state of retained tables.
// Non-table subjects are retained. Inputs remain unchanged.
//
// A standalone object that binds tables without belonging to one is kept
// whatever the selection; [Bindings.Select] decides it by those tables.
func Tables(objects schemaext.Objects, coverage schemaext.Coverage, keepTable func(schema, table string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool { return keepOwned(ref, keepTable) }
	return objects.Select(keep), coverage.SelectSubjects(keep)
}

// keepOwned keeps a table, or an object of a table, when keepTable keeps the
// table, and every other subject.
func keepOwned(ref objectidentity.ID, keepTable func(schema, table string) bool) bool {
	if !ref.Parent.Empty() {
		return keepTable(ref.Schema.Source, ref.Parent.Source)
	}
	if ref.Kind == objectidentity.KindTable {
		return keepTable(ref.Schema.Source, ref.Name.Source)
	}
	return true
}
