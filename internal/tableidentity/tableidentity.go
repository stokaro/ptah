// Package tableidentity keeps table pairing and captured child ownership on
// the same target-specific identity rules.
package tableidentity

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/descriptortable"
)

// verbatimTableNames holds each descriptor's Comparison.VerbatimTableNames.
// Subject runs once per table and column a comparison pairs, where copying the
// whole descriptor through platform.Lookup costs more than the pairing.
var verbatimTableNames = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Comparison.VerbatimTableNames
})

// Subject is the structured identity shared by table pairing and feature
// parent resolution. A dialect whose descriptor states VerbatimTableNames,
// such as SQLite, preserves exact catalog bytes at this boundary.
func Subject(schema, name, dialect string, semantics identifier.Semantics) objectidentity.ID {
	if !verbatimTableNames.Get(dialect) {
		return objectidentity.NewBuilder(semantics).TableParts(schema, name)
	}
	return objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name)
}
