package compare

import (
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/constraintscope"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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

// newConstraintKey builds a key for a table-level constraint, folding the
// constraint's NAME by the rule the engine resolves one under.
//
// It is a separate constructor rather than a parameter on [newTableMemberKey]
// because that helper's other callers key a COLUMN, and the two rules disagree
// on exactly the targets where a constraint name is not a column name. Measured
// on `mysql:8.4` and `mariadb:11.4`: a constraint created as `FK_A` is dropped
// by `ALTER TABLE child DROP FOREIGN KEY fk_a`, and one created as `UQ_A` by
// `ALTER TABLE u DROP INDEX uq_a`. The column rule keeps that case and the index
// rule folds it, so keying a constraint as a column made the two spellings two
// objects: a drop and an add planned for one constraint, on every run, and drift
// reported for a database that matches (stokaro/ptah#2028).
//
// PostgreSQL is untouched by the change and correctly so: the two rules agree
// there, and an upper-case name in a PostgreSQL catalog was created quoted, so
// it really is a different object from the unquoted spelling.
func newConstraintKey(table, constraint string, semantics identifier.Semantics) tableMemberKey {
	return tableMemberKey{
		table:  newQualifiedTableIdentity(table, semantics),
		member: semantics.IndexIdentityKey(constraint),
	}
}

// recordSynthesized adds constraints the comparison derives — a field-level
// `check=`, a table's `checks` list, a table primary key, a field-level
// `foreign=` — to the desired-side map under the same identity key an explicit
// declaration would take.
//
// A declaration the author wrote wins over a derived one of the same name.
// Both spell one object, only one of them is what the author asked for, and
// the derived form carries no attribute the explicit one lacks.
func recordSynthesized(
	into map[tableMemberKey]schemamodel.Constraint,
	synthesized []schemamodel.Constraint,
	semantics identifier.Semantics,
) {
	for _, constraint := range synthesized {
		key := newConstraintKey(constraint.Table, constraint.Name, semantics)
		if _, declared := into[key]; declared {
			continue
		}
		into[key] = constraint
	}
}

// constraintIdentity is the same answer in the form a CONSUMER can read.
//
// The key above is an [objectidentity.Key], whose parts are deliberately
// unreachable: it exists to be compared, not to be taken apart. The derivation
// lives in [constraintscope] because the comparator is not the only producer of
// constraint changes -- the generator writes them too when it reverses a diff --
// and two derivations would be two rules for one question.
func constraintIdentity(
	table, constraint string,
	semantics identifier.Semantics,
) difftypes.ConstraintIdentity {
	return constraintscope.Identity(semantics, table, constraint)
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
