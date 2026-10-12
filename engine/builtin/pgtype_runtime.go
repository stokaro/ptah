package builtin

import (
	"ptah.run/engine"
	"ptah.run/internal/pgtypeprovider"
)

// pgtypeProvider assembles the PostgreSQL user-type owner as a provider of
// its own, as the extension owner is: a domain, a composite type and a range
// type are objects of the PostgreSQL family without the PostgreSQL target
// importing them. Its rendering joins the family's composition in
// [ownersFor].
//
// Its reports count the types under "domains", "composite_types" and
// "range_types", the names the common model's types are counted under, and
// the schema statistics add the two: a metric counts its objects whoever
// owns them.
func pgtypeProvider() engine.Provider {
	return pgtypeprovider.Provider()
}
