// Package tableidentity keeps table pairing and captured child ownership on
// the same target-specific identity rules.
package tableidentity

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
)

// Subject is the structured identity shared by table pairing and feature
// parent resolution. A dialect whose descriptor states VerbatimTableNames,
// such as SQLite, preserves exact catalog bytes at this boundary.
func Subject(schema, name, dialect string, semantics identifier.Semantics) objectidentity.ID {
	if described, _ := platform.Lookup(dialect); !described.Comparison.VerbatimTableNames {
		return objectidentity.NewBuilder(semantics).TableParts(schema, name)
	}
	return objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name)
}
