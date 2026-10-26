package deporder

import (
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableref"
)

// NamesRowType reports whether spelled, a type as a catalog prints it or a
// routine's definition writes it, is the row type of relation or an array of
// it. Every table and view of PostgreSQL has a row type of its own name, which
// a composite attribute, a domain's base type or a routine's parameter can
// name; the server then records a dependency on the relation.
//
// Names compare as [ReferencesRelation] compares them, and resolve as it
// resolves them: a relation named without a schema is in
// semantics.DefaultSchema, and a type spelled without one may be in any
// schema of semantics.SearchPath, since PostgreSQL prints a type without its
// schema wherever the path reaches it. What semantics leaves out matches any
// schema, so a type that might be the relation's row type counts as one.
//
// A spelling [tableref] does not accept, such as `character varying(10)`,
// names no row type.
func NamesRowType(spelled, relation string, semantics identifier.Semantics) bool {
	spelled = strings.TrimSpace(spelled)
	for strings.HasSuffix(spelled, "[]") {
		spelled = strings.TrimSpace(strings.TrimSuffix(spelled, "[]"))
	}
	named, ok := tableref.Parse(spelled)
	if !ok {
		return false
	}
	ref, ok := tableref.Parse(relation)
	if !ok || foldName(named.Name) != foldName(ref.Name) {
		return false
	}
	name := dottedName{foldName(named.Name)}
	if named.Qualified {
		name = dottedName{foldName(named.Schema), foldName(named.Name)}
	}
	return namesSchema(name, ref, sessionResolution(semantics))
}
