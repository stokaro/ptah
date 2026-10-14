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
// The PostgreSQL-family reader and every source (Go annotations, YAML, HCL and
// SQL) produce its policy and table-state values, so comparison, planning and
// rendering of row-level security run through these services.
func pgpolicyProvider() engine.Provider {
	return pgpolicyprovider.Provider()
}
