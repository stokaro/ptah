package builtin

import (
	"slices"

	"ptah.run/core/renderer"
	"ptah.run/engine"
	"ptah.run/feature/oratype"
	"ptah.run/feature/synonym"
	"ptah.run/internal/oratypeprovider"
)

// oratypeProvider assembles the Oracle user type owner as a provider of its
// own: a domain and an object type are objects of the Oracle target without
// the Oracle target importing them. Its rendering joins the target's
// composition in [ownersFor] through [oracleRegistry].
//
// Its reports count the types under "domains" and "composite_types", the
// names the PostgreSQL owner counts its types under: a metric counts its
// objects whoever owns them.
func oratypeProvider() engine.Provider {
	return oratypeprovider.Provider()
}

// oracleRegistry is the Oracle owners' handler registry: synonyms, and
// domains and object types.
func oracleRegistry() (renderer.Extensions, error) {
	return renderer.NewExtensions(slices.Concat(synonym.Handlers(), oratype.Handlers())...)
}
