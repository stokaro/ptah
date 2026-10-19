package descriptor

import (
	"fmt"
	"slices"
)

// SystemSchemas states the schemas a PostgreSQL-family engine owns beyond
// the ones every member of the family owns, information_schema and the pg_
// namespaces, which internal/pgsystemschema holds. The reader takes that base
// set and adds these lists. The zero value adds nothing, as on PostgreSQL.
//
// A name is matched exactly, so a quoted lookalike in another case is a
// schema of the user's. A schema may be in more than one list, since each
// list states one thing a reader does with it.
type SystemSchemas struct {
	// Catalog are the schemas that hold the server's own catalog, which a
	// realm read leaves out of a description.
	Catalog []string `json:"catalog,omitempty"`
	// Reserved are the schemas the server owns that a schema document may
	// not hold: a declaration of one is refused, a described state that
	// holds one is not compared, an object placed in one names the schema
	// rather than a schema block of the document, and a migration never
	// creates one.
	Reserved []string `json:"reserved,omitempty"`
	// Implicit are the schemas every database of the engine holds and the
	// server refuses to create, so a migration places objects in them without
	// creating them first. A document may declare one.
	Implicit []string `json:"implicit,omitempty"`
}

// validateSystemSchemas checks that every system schema has a name, that no
// list names one twice, and that no schema is both reserved and implicit:
// a reserved schema is never created, so the implicit entry would say
// nothing.
func (d Dialect) validateSystemSchemas() error {
	lists := []struct {
		name    string
		schemas []string
	}{
		{"catalog", d.SystemSchemas.Catalog},
		{"reserved", d.SystemSchemas.Reserved},
		{"implicit", d.SystemSchemas.Implicit},
	}
	for _, list := range lists {
		for i, schema := range list.schemas {
			if schema == "" {
				return fmt.Errorf("%w: dialect %q: an empty %s system schema", ErrInvalidDescriptor, d.Name, list.name)
			}
			if slices.Contains(list.schemas[:i], schema) {
				return fmt.Errorf("%w: dialect %q: %s system schema %q is named twice", ErrInvalidDescriptor, d.Name, list.name, schema)
			}
		}
	}
	for _, schema := range d.SystemSchemas.Implicit {
		if slices.Contains(d.SystemSchemas.Reserved, schema) {
			return fmt.Errorf("%w: dialect %q: system schema %q is both reserved and implicit; a reserved schema is never created",
				ErrInvalidDescriptor, d.Name, schema)
		}
	}
	return nil
}
