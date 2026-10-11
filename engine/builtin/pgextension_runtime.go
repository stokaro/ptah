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
// No reader or source produces an extension value yet, so a comparison, a
// plan or a render reaches these services only with values a caller built
// itself; the common extension path still plans and renders what a
// declaration asks for.
//
// The owner's reports are left out until the common extension model is
// removed. Both count an extension under the metric name "extensions", and
// the schema statistics refuse a metric that two reports define, so selecting
// both would fail every statistics run while the common count still answers
// for every extension a source declares.
func pgextensionProvider() engine.Provider {
	provider := pgextensionprovider.Provider()
	provider.Reporting = nil
	return provider
}
