package builtin

import (
	"ptah.run/engine"
	"ptah.run/internal/pgpolicyprovider"
)

// pgpolicyProvider assembles the PostgreSQL row-security owner as a provider of
// its own, as TimescaleDB is: its models attach to PostgreSQL-family tables
// without the PostgreSQL target importing them. Its rendering joins the
// family's composition in [ownersFor].
//
// No reader or source produces a policy or table-state value yet, so a
// comparison, a plan or a render reaches these services only with values a
// caller built itself; the common row-level security path still renders what a
// declaration asks for.
func pgpolicyProvider() engine.Provider {
	return pgpolicyprovider.Provider()
}
