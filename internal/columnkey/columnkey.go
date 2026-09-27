// Package columnkey names the key a column's own UNIQUE builds, as the server
// names it, on the engines whose naming is measured.
//
// The schema comparison and the migration planners need one answer. The
// comparison reads the database key under that name as the column's own, and
// plans every other key over the column for removal. A planner drops a removed
// key that holds the name before the column takes its key; otherwise the server
// gives the column's key another name, and the next comparison plans it again.
// Two copies of the rule would agree when the second is written and drift when
// the first changes.
//
// Measured on MySQL 8.4.11 and 26.7.0, MariaDB 11.8.9 and 12.3.3 and
// PostgreSQL 18.6, with Atlas CE v1.3.0 comparing a file that writes `x int
// UNIQUE` on table c with a database whose one key over x has another name:
//
//	key in the database   MySQL, MariaDB                     PostgreSQL
//	c_x_uq                DROP INDEX c_x_uq, ADD ... x (x)   DROP c_x_uq, ADD c_x_key
//	x                     synced                             DROP x, ADD c_x_key
//	c_x_key               DROP INDEX c_x_key, ADD ... x (x)  synced
package columnkey

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/pgname"
)

// Named reports whether the package names a column's own UNIQUE on dialect.
// On another engine the naming is not measured, and a caller keeps the
// column's uniqueness with whichever key covers the column.
func Named(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.Postgres:
		return true
	default:
		return false
	}
}

// Name answers the name the server of dialect gives the own UNIQUE of column
// on table, and false where [Named] is false. table is the bare relation name,
// without its schema. taken reports whether another key of the table holds a
// name; a nil taken holds none.
//
// MySQL and MariaDB name the key after the column, with `_2` and on where
// another key of the table holds that name; see [mysqlname.IndexName]. taken
// answers for the table's index names there.
//
// PostgreSQL names it `<table>_<column>_key`, with `1` and on where a relation
// or a constraint of the schema holds that name; see [pgname.Constraint].
// taken answers for both namespaces of the table's schema there; see
// [pgname.ConstraintNames] and [pgname.RelationNames]. Measured on PostgreSQL
// 18.6, `ALTER TABLE c ADD COLUMN x int UNIQUE` beside an index c_x_key over y
// names the key c_x_key1.
func Name(dialect, table, column string, taken func(string) bool) (string, bool) {
	if taken == nil {
		taken = func(string) bool { return false }
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return mysqlname.IndexName(dialect, column, taken), true
	case platform.Postgres:
		return pgname.Constraint(table, []string{column}, "key", taken), true
	default:
		return "", false
	}
}

// Same reports whether the server of dialect reads two key names as one. MySQL
// and MariaDB compare index names without case, so a key named `X` over x is
// the column's own there; Atlas CE v1.3.0 renames it to `x`, which changes
// nothing the server tells apart. Other engines compare the names as written.
func Same(dialect, a, b string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return strings.EqualFold(a, b)
	default:
		return a == b
	}
}

// Shares reports whether a constraint of kind, on the column's table, holds a
// name the column's own key cannot take on dialect.
//
// On MySQL and MariaDB a UNIQUE constraint is an index, and a table's index
// names are one namespace; a CHECK and a foreign key name another, so a CHECK
// named `x` leaves the key `x` free. On PostgreSQL every constraint of a table
// shares one namespace: ADD CONSTRAINT c_x_key UNIQUE (x) beside a CHECK named
// c_x_key answers `constraint "c_x_key" for relation "c" already exists`.
// Measured on MySQL 8.4.11 and PostgreSQL 18.6.
func Shares(dialect, kind string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return strings.EqualFold(kind, "UNIQUE")
	case platform.Postgres:
		return true
	default:
		return false
	}
}
