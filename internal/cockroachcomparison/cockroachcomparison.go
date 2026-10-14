// Package cockroachcomparison holds the rules CockroachDB gives the shared
// schema comparison ([schemacomparison.Rules]): how an index definition reads
// on CockroachDB. The built-in runtime registers them on its CockroachDB
// target, so the shared comparison holds no CockroachDB knowledge of its own.
package cockroachcomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules returns CockroachDB's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}}
}

// Indexes compares CockroachDB index definitions.
//
// Registering the rules has the shared comparison compare the definition the
// reader reports: the uniqueness, each key's column or expression and its
// direction, the STORING columns, and the access method below. A changed
// definition is a DROP INDEX and a CREATE INDEX, which CockroachDB v26.3.2
// runs in one transaction under one name.
//
// Three properties of an index are left out, because CockroachDB does not
// report what was declared for them, measured on v26.3.2:
//
//   - The NULLS ordering. CockroachDB has no NULLS FIRST or NULLS LAST in an
//     index key (`CREATE INDEX i ON t (b NULLS LAST)` answers "unimplemented:
//     this syntax") and orders NULLs first in an ascending key and last in a
//     descending one, so the ordering follows the direction.
//   - The operator class. pg_index.indclass is 0 for every key and pg_opclass
//     is empty, also for an index built with gin_trgm_ops.
//   - The storage parameters. `WITH (fillfactor = 70)` is accepted and stored
//     nowhere: pg_class.reloptions stays empty.
type Indexes struct{}

// OptionsEqual reports whether a CockroachDB index has the access method
// declared for it. An index has no other option the reader reports.
func (Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	return accessMethod(desired.Type) == accessMethod(current.Method)
}

// RenameCarries reports that a CockroachDB index keeps every facet through a
// rename: ALTER INDEX ... RENAME TO changes the name and nothing else.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return true
}

// accessMethod reduces an access method to the name CockroachDB reports for
// it. No method is the default, btree. CockroachDB builds GIN, GIST and its
// own INVERTED as one kind of index, and pg_get_indexdef prints each of them
// as `USING gin`; the reader reports pg_am's prefix and inverted as btree and
// gin for the same reason.
func accessMethod(method string) string {
	switch normalized := strings.ToLower(strings.TrimSpace(method)); normalized {
	case "", "btree", "prefix":
		return "btree"
	case "gin", "gist", "inverted":
		return "gin"
	default:
		return normalized
	}
}
