// Package spannercomparison holds the rules Spanner gives the shared schema
// comparison ([schemacomparison.Rules]): how an index definition reads on
// Spanner's PostgreSQL interface. The built-in runtime registers them on its
// Spanner target, so the shared comparison holds no Spanner knowledge of its
// own.
package spannercomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules returns Spanner's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}}
}

// Indexes compares Spanner index definitions.
//
// Registering the rules has the shared comparison compare the definition the
// reader takes from information_schema: the uniqueness, each key's column and
// direction, the STORING columns, and each key's NULLS ordering below. A
// changed definition is a DROP INDEX and a CREATE INDEX.
//
// Nothing else of an index is compared, because Spanner refuses it in CREATE
// INDEX, measured on the emulator behind PGAdapter v0.56.1: an access method
// ("Setting access method is not supported"), an operator class ("Setting
// operator class is not supported") and storage parameters ("Index options are
// not supported"). An expression key is refused as well, and the shared
// comparison reads keys as it reads them on every target.
type Indexes struct{}

// OptionsEqual reports whether each key of a Spanner index has the NULLS
// ordering declared for it.
func (Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	declared := declaredOrderings(desired)
	reported := reportedOrderings(current)
	if len(declared) != len(reported) {
		// A different number of keys is the shared comparison's to report.
		return true
	}
	for position, nullsFirst := range declared {
		if nullsFirst != reported[position] {
			return false
		}
	}
	return true
}

// RenameCarries reports that a Spanner index keeps every facet through a
// rename: ALTER INDEX ... RENAME TO changes the name and nothing else.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return true
}

// declaredOrderings resolves the NULLS ordering of each declared key.
func declaredOrderings(index schemamodel.Index) []bool {
	if len(index.Parts) == 0 {
		return make([]bool, len(index.Fields))
	}
	orderings := make([]bool, len(index.Parts))
	for position, part := range index.Parts {
		orderings[position] = nullsFirst(part.NullsOrder, part.Desc)
	}
	return orderings
}

// reportedOrderings resolves the NULLS ordering of each key the reader
// reports.
func reportedOrderings(index catalog.Index) []bool {
	if len(index.Parts) == 0 {
		return make([]bool, len(index.Columns))
	}
	orderings := make([]bool, len(index.Parts))
	for position, part := range index.Parts {
		orderings[position] = nullsFirst(part.NullsOrder, part.Desc)
	}
	return orderings
}

// nullsFirst resolves a key's NULLS ordering to the one the server applies.
// Spanner orders NULLs as PostgreSQL does: last in an ascending key and first
// in a descending one, unless the key says otherwise. It reports the ordering
// only where the key contradicts that default, so an omitted ordering and an
// explicit spelling of the default are one key.
func nullsFirst(order string, desc bool) bool {
	switch strings.ToUpper(strings.TrimSpace(order)) {
	case catalog.NullsOrderFirst:
		return true
	case catalog.NullsOrderLast:
		return false
	default:
		return desc
	}
}
