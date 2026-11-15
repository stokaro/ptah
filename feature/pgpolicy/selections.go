package pgpolicy

import "ptah.run/core/selectext"

// Selections lets Atlas-style selectors name provider-owned policies. A policy
// is named through its table, since its name is unique only there: an
// exclusion takes `notes.notes_owner[type=policy]` or
// `public.notes.notes_owner[type=policy]`, and an include keeps a policy with
// its table.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: PolicyKind, Type: "policy", Naming: selectext.InSchema},
	}}
}
