// Package featureselect keeps feature objects and knowledge aligned with common table selection.
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
func Tables(objects schemaext.Objects, coverage schemaext.Coverage, keepTable func(schema, table string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		if !ref.Parent.Empty() {
			return keepTable(ref.Schema.Source, ref.Parent.Source)
		}
		if ref.Kind == objectidentity.KindTable {
			return keepTable(ref.Schema.Source, ref.Name.Source)
		}
		return true
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}
