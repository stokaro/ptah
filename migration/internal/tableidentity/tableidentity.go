// Package tableidentity keeps table pairing and captured child ownership on
// the same target-specific identity rules.
package tableidentity

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
)

// Subject is the structured identity shared by table pairing and feature
// parent resolution. SQLite preserves exact catalog bytes at this boundary.
func Subject(schema, name, dialect string, semantics identifier.Semantics) objectidentity.ID {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return objectidentity.NewBuilder(semantics).TableParts(schema, name)
	}
	if schema == "" {
		schema = semantics.DefaultSchema
	}
	return objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name)
}
