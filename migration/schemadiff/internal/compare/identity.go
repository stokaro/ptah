package compare

import (
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/tableref"
)

// tableMemberKey identifies something owned by a table -- a constraint, a
// trigger, a column -- by that table and its own name.
//
// The table component is a normalized [tableIdentity] rather than the string it
// was written as, because the two sides of a comparison do not spell the same
// table the same way. A schema read out of a database reports the table's
// schema as empty wherever the engine treats it as implicit, while a schema
// built from Go annotations or from HCL carries it explicitly. So `t` and
// `main.t` arrive as two spellings of one table.
//
// Keying by the raw string made every constraint the database has look absent
// from the desired schema, and therefore removed. On SQLite that surfaced as
// `rebuilding table t requires the retained table definition`; on PostgreSQL it
// silently emitted `ALTER TABLE "users" DROP CONSTRAINT "users_pkey"` on the
// second apply of an unchanged file, exited 0, and reported success
// (stokaro/ptah#1232).
//
// Table comparison never had the defect because it has always keyed through
// [newTableIdentity]. Putting the same normalization inside this type rather
// than at each use site is deliberate: there are seventeen construction sites
// across four files, and a normalization the caller has to remember is one the
// next caller will not.
type tableMemberKey struct {
	table  tableIdentity
	member string
}

// newTableMemberKey builds a key from a table name that may or may not carry a
// schema, resolving an absent one to the dialect's default.
func newTableMemberKey(table, member string, semantics identifier.Semantics) tableMemberKey {
	return tableMemberKey{
		table:  newQualifiedTableIdentity(table, semantics),
		member: member,
	}
}

// newQualifiedTableIdentity normalizes a table name written as one string,
// which is how both the desired schema and the database report it.
//
// Parsing is delegated to tableref so a table whose own name contains a dot --
// quoted as `"tenant.data"` -- is not mistaken for a schema-qualified one.
func newQualifiedTableIdentity(table string, semantics identifier.Semantics) tableIdentity {
	ref, ok := tableref.Parse(table)
	if !ok {
		return newTableIdentity("", table, semantics)
	}
	return newTableIdentity(ref.Schema, ref.Name, semantics)
}
