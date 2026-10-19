// Package spannercomparison holds the rules Spanner gives the shared schema
// comparison ([schemacomparison.Rules]): how an index definition reads on
// Spanner's PostgreSQL interface, and which index Spanner built for a foreign
// key. The built-in runtime registers them on its Spanner target, so the
// shared comparison holds no Spanner knowledge of its own.
package spannercomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules returns Spanner's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}, ForeignKeyIndexes: ForeignKeyIndexes{}}
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

// ForeignKeyIndexes finds the index Spanner built to enforce a foreign key.
//
// Spanner creates it and names it itself -- `IDX_children_parent_id_FBF4366D...`
// -- so unlike MySQL's, which carries the constraint's name, it cannot be found
// by identity. What identifies it is its key: the constraint's columns, in the
// constraint's order, which is what the enforcement needs.
//
// Without it the index is an object in the database that no desired state
// declares, so the comparison plans a DROP for it, and the server refuses:
// measured on the PGAdapter emulator v0.55.2, the second apply of a document
// containing one foreign key answered
//
//	Cannot drop index `IDX_children_parent_id_FBF4366D73F2084A`.
//	It is in use by foreign keys: `children_parent_fk`.
//
// so such a document applied exactly once and failed on every run afterwards,
// with a plan that never changed (stokaro/ptah#2076).
//
// A user's own index over the same columns is not swallowed by this: an
// identity the desired state declares as an index is compared as an index
// whatever this answers.
type ForeignKeyIndexes struct{}

// BackingIndexes returns the indexes of current that are the key Spanner
// built for key.
func (ForeignKeyIndexes) BackingIndexes(
	current *catalog.Database,
	key catalog.Constraint,
	semantics identifier.Semantics,
) []catalog.Index {
	columns := uniqueStringsPreserveOrder(key.ColumnNamesOrDefault())
	if len(columns) == 0 {
		return nil
	}
	table := key.QualifiedTableName()
	backing := make([]catalog.Index, 0, 1)
	for _, index := range current.Indexes {
		if indexEnforces(index, table, columns, semantics) {
			backing = append(backing, index)
		}
	}
	return backing
}

// indexEnforces reports whether one index is the key Spanner built for a
// foreign key on table.
//
// Two signals, and both are needed. The columns say the index COULD enforce the
// constraint; the name says Spanner made it. A user's own index over the same
// columns is a different object -- measured on the emulator, declaring one
// leaves Spanner's beside it rather than reusing it -- and it stays droppable,
// which it would not if the columns alone decided.
//
// A primary key's index is excluded because it is already owned, and because a
// foreign key whose columns are the primary key is enforced by that key rather
// than by an index of its own.
func indexEnforces(
	index catalog.Index,
	table string,
	columns []string,
	semantics identifier.Semantics,
) bool {
	if index.IsPrimary || !generatedIndexName(index.Name) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(index.QualifiedTableName()), strings.TrimSpace(table)) {
		return false
	}
	return sameColumnNames(semantics, columns, index.Columns)
}

// generatedIndexName reports whether a name is one Spanner wrote for itself:
// `IDX_<table>_<columns>_<16 hex digits>`, as in
// `IDX_children_parent_id_FBF4366D73F2084A`.
//
// Only the two ends are checked. The middle is the table and the columns, which
// the caller compares properly rather than by string surgery, and a name
// carrying an underscore in an identifier would defeat any attempt to split it.
func generatedIndexName(name string) bool {
	trimmed := strings.TrimSpace(name)
	const suffixDigits = 16
	if !strings.HasPrefix(trimmed, "IDX_") || len(trimmed) < len("IDX_")+1+suffixDigits {
		return false
	}
	suffix := trimmed[len(trimmed)-suffixDigits:]
	if trimmed[len(trimmed)-suffixDigits-1] != '_' {
		return false
	}
	for i := range len(suffix) {
		if !isHexDigit(suffix[i]) {
			return false
		}
	}
	return true
}

// isHexDigit reports whether b is one of 0-9, a-f or A-F.
func isHexDigit(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

// sameColumnNames compares two column lists in order, under the target's
// column-name comparison.
func sameColumnNames(semantics identifier.Semantics, left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for position, name := range left {
		if semantics.ColumnIdentityKey(name) != semantics.ColumnIdentityKey(right[position]) {
			return false
		}
	}
	return true
}

// uniqueStringsPreserveOrder drops the repeats of values, keeping the first.
func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
