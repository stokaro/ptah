package pgpolicy

import "ptah.run/core/selectext"

// Selections lets Atlas-style selectors name provider-owned policies.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: PolicyKind, Type: "policy", Naming: selectext.InSchema},
	}}
}
