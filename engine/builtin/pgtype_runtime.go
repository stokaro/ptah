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
// No source or reader produces a user-type value yet, so the common user-type
// path still plans and renders what a declaration asks for. The owner's
// reports stay out until the common user-type model goes: both count the
// types under the metric names "domains", "composite_types" and
// "range_types", and the schema statistics refuse a metric two reports
// define.
func pgtypeProvider() engine.Provider {
	provider := pgtypeprovider.Provider()
	provider.Reporting = nil
	return provider
}
