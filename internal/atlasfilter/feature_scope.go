package atlasfilter

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Table-owned feature objects and their knowledge records follow the same
// parent selection. Filtering either alone loses retained state or leaves an
// unknown child attached to a table outside the comparison. Kind-wide knowledge
// remains the source's claim, including namespaces with no concrete values.
func selectTableFeatures(objects schemaext.Objects, coverage schemaext.Coverage, keepTable func(schema, table string) bool) (schemaext.Objects, schemaext.Coverage) {
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
