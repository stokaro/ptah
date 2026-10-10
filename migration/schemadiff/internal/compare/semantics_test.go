package compare_test

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
)

// postgresSemantics is the identifier rule the comparison runs under: an
// unqualified name resolves to the default schema, which is what makes a
// document naming `schema.public` and a read reporting none the same object.
func postgresSemantics() identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	return semantics
}
