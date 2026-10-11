package builtin

import (
	"ptah.run/engine"
	"ptah.run/internal/pgextensionprovider"
)

// pgextensionProvider assembles the PostgreSQL extension owner as a provider
// of its own, as the row-security owner is: an extension is a database-wide
// object of the PostgreSQL family without the PostgreSQL target importing it.
// Its rendering joins the family's composition in [ownersFor].
//
// Every source and the PostgreSQL reader hand extensions to this owner, and
// its reports count them under the metric name "extensions".
func pgextensionProvider() engine.Provider {
	return pgextensionprovider.Provider()
}
