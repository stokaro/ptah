// Package serverobjects answers which objects a database server creates in
// every database it makes, by dialect and name.
//
// Such an object belongs to the server. A declaration rarely names it, and
// reading its absence from the declaration as a request to drop it plans a
// statement the server refuses, or one that removes something the server's own
// objects are built on. The package is a leaf so that the comparison and the
// writers that clean a database can ask the same question without a
// connection.
package serverobjects

import (
	"slices"

	"ptah.run/core/platform"
)

// yugabyteDBExtensions are the extensions YugabyteDB installs, into
// pg_catalog and owned by the bootstrap superuser, in every database it
// creates, including one made by CREATE DATABASE.
//
// Measured 2026-09-26: 2024.2.11 and 2025.2.6 install pg_stat_statements, and
// 2026.1.2 installs pg_stat_statements and postgres_fdw. On 2026.1.2 neither
// can be dropped: the gv$ foreign tables of the built-in yb_global_views_server
// depend on both, and DROP EXTENSION answers "cannot drop extension ...
// because other objects depend on it" (stokaro/ptah#3687, stokaro/ptah#3693).
//
// It is the one list of them. The comparison reads it so that it never plans
// their removal, and the realm cleanup reads it so that it never attempts one.
// Two copies agreed when the second was written and would stop agreeing when
// the next line adds an extension to one of them.
var yugabyteDBExtensions = []string{"pg_stat_statements", "postgres_fdw"}

// Extensions returns the extensions the server a dialect names installs in
// every database it creates, sorted by name, in a slice the caller owns. A
// dialect with no such extensions returns none.
//
// PostgreSQL's plpgsql is not among them: a PostgreSQL user may drop it, and
// the comparison leaves it alone through the default ignore list, which a
// caller can clear.
func Extensions(dialect string) []string {
	if platform.NormalizeDialect(dialect) != platform.YugabyteDB {
		return nil
	}
	return slices.Sorted(slices.Values(yugabyteDBExtensions))
}

// IsExtension reports whether name is one of [Extensions] for the dialect. The
// name is compared exactly, as the catalog reports it.
func IsExtension(dialect, name string) bool {
	return slices.Contains(Extensions(dialect), name)
}
