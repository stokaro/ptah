package pgtype

import "ptah.run/core/selectext"

// Selections is how a command's scope names the owner's types: a domain, a
// composite type and a range type each by its own name, in the schema that
// holds it, with the selector types the common types use: `domain`,
// `composite_type` and `range`. An exclusion subtracts no field from one.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: DomainKind, Type: "domain", Naming: selectext.InSchema},
		{Kind: CompositeKind, Type: "composite_type", Naming: selectext.InSchema},
		{Kind: RangeKind, Type: "range", Naming: selectext.InSchema},
	}}
}
