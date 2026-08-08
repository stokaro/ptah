// Package lintdialect defines the SQL dialects accepted by migration linting.
// It keeps lint-policy and command validation on one compatibility boundary.
package lintdialect

// Expected is the user-facing list of supported lint dialects.
const Expected = "postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, or spanner"

// Valid reports whether dialect is supported. The empty value means that the
// hybrid lint scanner should run every dialect-independent rule.
func Valid(dialect string) bool {
	switch dialect {
	case "", "postgres", "mysql", "mariadb", "sqlite", "clickhouse", "cockroachdb", "yugabytedb", "spanner":
		return true
	default:
		return false
	}
}
