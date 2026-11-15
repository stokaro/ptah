// Package featureselect keeps feature objects and knowledge aligned with common
// table selection, including the objects that bind tables without belonging to
// one, which their owners' relation discovery describes.
package featureselect

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Tables selects table-owned feature objects and their knowledge with the same
// parent selection. keepTable receives an empty schema for a table the source
// did not qualify, and resolves it with its own default. Filtering either alone loses retained state or leaves an
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
//
// keepTable gets the schema the source wrote, and the empty string for one
// the identity's builder filled in. A reader and a source build identities
// before any connection is known, so a defaulted schema holds the offline
// default, `public` on PostgreSQL, whatever the session's default is. Passed
// on, a policy the read found in `reporting` under `search_path=reporting`
// names a `public` table, and a scope that keeps the table leaves the policy
// out.
func keepOwned(ref objectidentity.ID, keepTable func(schema, table string) bool) bool {
	if OfTable(ref) {
		return keepTable(ref.Schema.Authored(), ref.Parent.Source)
	}
	if ref.Kind == objectidentity.KindTable {
		return keepTable(ref.Schema.Authored(), ref.Name.Source)
	}
	return true
}

// OfTable reports whether ref is an object of a table, such as a policy on
// one: an identity with a parent. [Tables] and [Bindings.Select] keep it or
// leave it out with that table, so a scope that has selected through them
// has decided it. A positive selection must not ask its selectors to name
// the object again: an include names a table's children only through the
// table, and offering a child's own name to the selectors drops it from a
// table the selection keeps.
func OfTable(ref objectidentity.ID) bool {
	return !ref.Parent.Empty()
}
