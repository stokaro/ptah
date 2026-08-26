package compare

import (
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
)

// foreignKeyConstraintChanged compares FOREIGN KEY constraint definitions.
//
// Referential actions are normalized before comparison (see
// normalizeReferentialAction): databases report the default action as
// "NO ACTION" via information_schema.referential_constraints, whereas a Go
// annotation that omits on_delete/on_update leaves the value empty. Without
// the normalization the two would never match for FKs declared without an
// explicit action, producing a perpetual drop+add loop on every `generate`
// (the same hazard checkConstraintChanged guards against for CHECK clauses).
func foreignKeyConstraintChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
) bool {
	// Compare local columns, under the dialect's column-name comparison rather
	// than as raw strings: Oracle stores an unquoted `author_id` as AUTHOR_ID,
	// and comparing the spellings made an untouched foreign key read as
	// changed.
	if !sameColumnNames(semantics, genConstraint.Columns, uniqueStringsPreserveOrder(dbConstraint.ColumnNamesOrDefault())) {
		return true
	}

	// Compare referenced table
	if !foreignTableRefMatches(genConstraint.ForeignTable, dbConstraint, semantics) {
		return true
	}

	// Compare referenced columns, the same way.
	if !sameColumnNames(semantics, genConstraint.ForeignColumnsOrDefault(), uniqueStringsPreserveOrder(dbConstraint.ForeignColumnsOrDefault())) {
		return true
	}

	// Compare delete rule
	if normalizeReferentialAction(genConstraint.OnDelete, dialect) != normalizeReferentialAction(getStringValue(dbConstraint.DeleteRule), dialect) {
		return true
	}

	// Compare update rule
	if normalizeReferentialAction(genConstraint.OnUpdate, dialect) != normalizeReferentialAction(getStringValue(dbConstraint.UpdateRule), dialect) {
		return true
	}

	// Compare deferral. Without this a schema that declares DEFERRABLE against a
	// constraint created without it reports no difference, so the plan is empty
	// and the property never arrives (stokaro/ptah#1624).
	if genConstraint.Deferrable != dbConstraint.Deferrable {
		return true
	}
	if normalizeDeferralTiming(genConstraint.Initially) != normalizeDeferralTiming(dbConstraint.Initially) {
		return true
	}

	return false
}

// foreignTableRefMatches reports whether a declared referenced table names the
// same table the catalog recorded.
//
// The comparison is the dialect's own, not a string equality. Oracle folds an
// unquoted name to upper case, so a declaration referencing `ora_authors` and a
// catalog reporting ORA_AUTHORS name one table -- and comparing the strings
// made a foreign key nobody touched read as changed, which the diff expresses
// as a drop and an add of the same constraint on every run (stokaro/ptah#1875).
// The semantics are the CALLER's, not a second set derived from the dialect
// name. Re-deriving them here made this comparison disagree with the identity
// keys that paired the two constraints in the first place whenever the two
// sources disagreed about the dialect -- the same split #1244 fixed for the
// member keys.
func foreignTableRefMatches(
	generated string,
	dbConstraint catalog.Constraint,
	semantics identifier.Semantics,
) bool {
	generated = strings.TrimSpace(generated)
	if generated == "" {
		return dbConstraint.ForeignTable == nil
	}
	// Qualified on BOTH sides, resolving an unqualified name to the dialect's
	// default schema, because the two sides qualify differently and neither
	// spelling is wrong.
	//
	// A reader blanks the schema for the one it was scoped to, so the catalog
	// reports `parent`. `schema inspect` writes the declaration qualified, so
	// the description says `public.parent`. Comparing those as bare strings
	// made a composite foreign key differ from itself: `ptah schema apply` of a
	// description against the database it was read from planned a DROP and an
	// ADD on every run, taking a validating lock each time and never reporting
	// the schema as synced.
	//
	// A single-column key escaped it only by accident -- a declaration carries
	// that one on the field, as `parent(id)`, which is unqualified and happened
	// to match. This is the same resolution constraintIdentity already applies,
	// which is why the two sides paired as one object while their bodies
	// compared unequal (stokaro/ptah#2219).
	return semantics.QualifiedTableIdentityKey(generated) ==
		semantics.QualifiedTableIdentityKey(dbConstraint.QualifiedForeignTableName())
}

// sameColumnNames compares two column lists in order, under the dialect's
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

// normalizeReferentialAction canonicalizes an ON DELETE / ON UPDATE action so
// that semantically identical values compare equal across the generated and
// introspected sides.
//
// SQL treats an omitted referential action as NO ACTION. PostgreSQL, MySQL and
// MariaDB all report the default through
// information_schema.referential_constraints, while a Go field annotation that
// simply omits on_delete/on_update yields an empty string. Trimming,
// upper-casing, and folding "" into "NO ACTION" makes those equivalent and
// keeps an unchanged FK a no-op on repeated runs.
//
// Dialect-specific RESTRICT handling: MariaDB reports an unspecified action as
// RESTRICT (PostgreSQL and MySQL report NO ACTION), and InnoDB treats RESTRICT
// and NO ACTION identically. For the MySQL family RESTRICT is therefore folded
// to NO ACTION so an unchanged FK does not loop drop+add forever. PostgreSQL
// distinguishes RESTRICT (checked immediately) from NO ACTION (deferrable) at
// DDL level, so the fold is NOT applied there — doing so would mask a genuine
// RESTRICT <-> NO ACTION change the user intended.
func normalizeReferentialAction(action, dialect string) string {
	normalized := strings.ToUpper(strings.TrimSpace(action))
	normalized = strings.ReplaceAll(normalized, "_", " ")
	if normalized == "" {
		return "NO ACTION"
	}
	if normalized == "RESTRICT" && isMySQLFamily(dialect) {
		return "NO ACTION"
	}
	return normalized
}

// isMySQLFamily reports whether the dialect is MySQL or MariaDB, which share the
// InnoDB referential-action semantics (RESTRICT == NO ACTION).
func isMySQLFamily(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb":
		return true
	default:
		return false
	}
}

// getStringValue safely extracts string value from a pointer, returning empty string if nil
func getStringValue(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

// normalizeDeferralTiming folds the unwritten clause onto the timing an engine
// defaults to, so a schema saying `deferrable = true` and a catalog reporting
// condeferred false are the same statement rather than a permanent difference.
func normalizeDeferralTiming(timing string) string {
	normalized := strings.ToLower(strings.TrimSpace(timing))
	if normalized == "" {
		return "immediate"
	}
	return normalized
}
