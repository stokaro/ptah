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
// schema, resolving an absent one to the dialect's default, and normalizes the
// member the way the table half is already normalized.
//
// The member half was compared verbatim, which is correct only where the engine
// compares names verbatim. Oracle does not: an unquoted name is folded to upper
// case, so a declaration writing `orders_total_check` and a catalog reporting
// ORDERS_TOTAL_CHECK are the same constraint, and comparing the two strings
// made every apply drop one and add the other.
func newTableMemberKey(table, member string, semantics identifier.Semantics) tableMemberKey {
	return tableMemberKey{
		table:  newQualifiedTableIdentity(table, semantics),
		member: semantics.ColumnIdentityKey(member),
	}
}

// tableConstraintKey is what makes two constraint declarations the same
// constraint. It is a separate type from [tableMemberKey] rather than a second
// constructor for it, so the compiler -- not a reader -- finds every map and
// every lookup that has to agree about which fold applies.
type tableConstraintKey struct {
	table  tableIdentity
	member string
}

// newTableConstraintKey folds a constraint name the way the engine resolves
// one, which is NOT the way it resolves a column.
//
// The two rules disagree on exactly the two dialects where it matters.
// Measured on MySQL 8.4 and MariaDB 11.4: a constraint created as `FK_A` is
// reported by the catalog as `FK_A`, is dropped by `DROP FOREIGN KEY fk_a`, and
// a second constraint named `uq_a` beside an existing `UQ_A` is refused with
// `Duplicate key name 'uq_a'`. One object, one namespace entry, resolved
// case-insensitively.
//
// Keyed by [identifier.Semantics.ColumnIdentityKey] -- the rule for a COLUMN --
// those two spellings were two objects, so the comparison planned a drop and an
// add for a constraint nobody had touched, on every run (stokaro/ptah#2028).
// IndexIdentityKey is the rule the planners and
// [objectidentity.Builder.ConstraintParts] already use, so this is the
// comparator joining them rather than a third answer.
//
// PostgreSQL is the control and keeps both names: measured on 17.11, `"UQ_A"`
// and `uq_a` coexist on one table, because an upper-case name in that catalog
// was created quoted. Its ColumnIdentityKey and IndexIdentityKey agree, so
// nothing about that dialect changes here.
func newTableConstraintKey(table, constraint string, semantics identifier.Semantics) tableConstraintKey {
	return tableConstraintKey{
		table:  newQualifiedTableIdentity(table, semantics),
		member: semantics.IndexIdentityKey(constraint),
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
