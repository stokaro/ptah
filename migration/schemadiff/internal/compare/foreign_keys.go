package compare

import (
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
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
func foreignKeyConstraintChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint, dialect string) bool {
	// Compare local columns.
	if !slices.Equal(genConstraint.Columns, uniqueStringsPreserveOrder(dbConstraint.ColumnNamesOrDefault())) {
		return true
	}

	// Compare referenced table
	if !foreignTableRefMatches(genConstraint.ForeignTable, dbConstraint) {
		return true
	}

	// Compare referenced columns.
	if !slices.Equal(genConstraint.ForeignColumnsOrDefault(), uniqueStringsPreserveOrder(dbConstraint.ForeignColumnsOrDefault())) {
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

	return false
}

func foreignTableRefMatches(generated string, dbConstraint types.DBConstraint) bool {
	generated = strings.TrimSpace(generated)
	if generated == "" {
		return dbConstraint.ForeignTable == nil
	}
	if strings.Contains(generated, ".") {
		return generated == dbConstraint.QualifiedForeignTableName()
	}
	return generated == getStringValue(dbConstraint.ForeignTable)
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
