package builtin

import (
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgextension"
)

// pgextensionProvider assembles the PostgreSQL extension owner as a provider
// of its own, as the row-security owner is: an extension is a database-wide
// object of the PostgreSQL family without the PostgreSQL target importing it.
// Its rendering joins the family's composition in [ownersFor].
//
// It registers the change and operation codecs alone. No reader, source or
// planner produces an extension value yet, so the common extension path still
// plans and renders what a declaration asks for; the model codecs and the
// services join when the owner plans.
func pgextensionProvider() engine.Provider {
	return engine.Provider{ID: pgextension.Owner,
		Codecs: slices.Concat([]schemaext.Codec{pgextension.ChangeCodec()}, pgextension.OperationCodecs())}
}
