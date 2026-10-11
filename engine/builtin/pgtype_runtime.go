package builtin

import (
	"slices"

	"ptah.run/engine"
	"ptah.run/feature/pgtype"
)

// pgtypeProvider assembles the PostgreSQL user-type owner as a provider of
// its own, as the extension owner is: a domain, a composite type and a range
// type are objects of the PostgreSQL family without the PostgreSQL target
// importing them. Its rendering joins the family's composition in
// [ownersFor].
//
// It registers the change and operation codecs alone. No reader, source or
// planner produces a user-type value yet, so the common user-type path still
// plans and renders what a declaration asks for; the model codecs and the
// services join when the owner plans.
func pgtypeProvider() engine.Provider {
	return engine.Provider{ID: pgtype.Owner, Codecs: slices.Concat(pgtype.ChangeCodecs(), pgtype.OperationCodecs())}
}
