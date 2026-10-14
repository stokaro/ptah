// Package yugabytecomparison holds the rules YugabyteDB gives the shared
// schema comparison ([schemacomparison.Rules]): how an index definition reads
// on YugabyteDB. The built-in runtime registers them on its YugabyteDB target,
// so the shared comparison holds no YugabyteDB knowledge of its own.
package yugabytecomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules returns YugabyteDB's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}}
}

// Indexes compares YugabyteDB index definitions.
//
// Registering the rules has the shared comparison compare the definition the
// reader reports: the uniqueness, each key's column or expression and its
// direction, the INCLUDE columns, and the options below. A changed definition
// is a DROP INDEX and a CREATE INDEX.
//
// The storage parameters are left out. YugabyteDB 2026.1.2 refuses every one
// Ptah writes for an index: fillfactor and deduplicate_items are unrecognized,
// a ybgin index does not support fastupdate, and BRIN, the method
// pages_per_range belongs to, is not supported. So no index on the server
// carries one to compare.
type Indexes struct{}

// OptionsEqual reports whether a YugabyteDB index has the access method, and
// each key the operator class and NULLS ordering, declared for it.
func (Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	if accessMethod(desired.Type) != accessMethod(current.Method) {
		return false
	}
	desiredKeys := declaredKeys(desired)
	currentKeys := reportedKeys(current)
	if len(desiredKeys) != len(currentKeys) {
		// A different number of keys is the shared comparison's to report.
		return true
	}
	for position, key := range desiredKeys {
		if key != currentKeys[position] {
			return false
		}
	}
	return true
}

// RenameCarries reports that a YugabyteDB index keeps every facet through a
// rename: ALTER INDEX ... RENAME TO changes the name and nothing else.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return true
}

// keyOptions is the operator class and the NULLS ordering of one key, in the
// form the two sides are compared in.
type keyOptions struct {
	operator   string
	nullsFirst bool
}

// declaredKeys reduces the keys of a declared index. A key takes the
// operator class the renderer writes for it: its own, or the index's where it
// has none.
func declaredKeys(index schemamodel.Index) []keyOptions {
	parts := index.Parts
	if len(parts) == 0 {
		parts = make([]schemamodel.IndexPart, len(index.Fields))
		for position, field := range index.Fields {
			parts[position] = schemamodel.IndexPart{Name: field}
		}
	}
	keys := make([]keyOptions, len(parts))
	for position, part := range parts {
		operator := part.Operator
		if strings.TrimSpace(operator) == "" {
			operator = index.Operator
		}
		keys[position] = keyOptions{
			operator:   strings.ToLower(strings.TrimSpace(operator)),
			nullsFirst: nullsFirst(part.NullsOrder, part.Desc),
		}
	}
	return keys
}

// reportedKeys reduces the keys of an index the server reports. A reader that
// reported columns alone reported no option for any of them.
func reportedKeys(index catalog.Index) []keyOptions {
	if len(index.Parts) == 0 {
		keys := make([]keyOptions, len(index.Columns))
		for position := range keys {
			keys[position] = keyOptions{nullsFirst: nullsFirst("", false)}
		}
		return keys
	}
	keys := make([]keyOptions, len(index.Parts))
	for position, part := range index.Parts {
		keys[position] = keyOptions{
			operator:   strings.ToLower(strings.TrimSpace(part.Operator)),
			nullsFirst: nullsFirst(part.NullsOrder, part.Desc),
		}
	}
	return keys
}

// nullsFirst resolves a key's NULLS ordering to the one the server applies.
// YugabyteDB orders NULLs as PostgreSQL does: last in an ascending key and
// first in a descending one, unless the key says otherwise. The reader records
// only an ordering against that default, so an omitted ordering and an
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

// accessMethod reduces an access method to the name YugabyteDB builds it
// under. No method is the default. YugabyteDB substitutes lsm for btree and
// for hash ("substituting access method "lsm" for "hash"", measured on
// 2026.1.2), and ybgin for gin, and the catalog reports what it built.
func accessMethod(method string) string {
	switch normalized := strings.ToLower(strings.TrimSpace(method)); normalized {
	case "", "btree", "hash", "lsm":
		return "lsm"
	case "gin", "ybgin":
		return "ybgin"
	default:
		return normalized
	}
}
