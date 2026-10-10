package builtin

import (
	"slices"

	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
)

// pgpolicyProvider assembles the PostgreSQL row-security owner as a provider of
// its own, as TimescaleDB is: its models attach to PostgreSQL-family tables
// without the PostgreSQL target importing them. Its rendering joins the
// family's composition in [ownersFor].
//
// It registers the change and operation codecs, which a saved plan and a diff
// report encode. The model codecs join with the comparison and planning that
// produce owner values: a registered desired model is one the schema census
// measures, and nothing can declare one yet.
func pgpolicyProvider() engine.Provider {
	return engine.Provider{
		ID:     pgpolicy.Owner,
		Codecs: slices.Concat(pgpolicy.ChangeCodecs(), pgpolicy.OperationCodecs()),
	}
}
