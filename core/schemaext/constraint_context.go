package schemaext

import (
	"slices"

	"ptah.run/core/objectidentity"
)

// ConstraintContext supplies the common definition a constraint's attached
// settings qualify. Columns are local column names in key order, under the
// request's identifier semantics. They may be empty for a CHECK constraint.
// Type is the common constraint kind, such as FOREIGN KEY or UNIQUE.
// A missing context means the definition was not supplied, not that the
// constraint has no columns. Contexts contain no feature values or SQL.
type ConstraintContext struct {
	Subject objectidentity.ID
	Type    string
	Columns []string
}

// Clone returns independent column storage. Identity components are values.
func (c ConstraintContext) Clone() ConstraintContext {
	c.Columns = slices.Clone(c.Columns)
	return c
}
