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
// because other objects depend on it" (stokaro/ptah#3687).
var yugabyteDBExtensions = []string{"pg_stat_statements", "postgres_fdw"}

// IsExtension reports whether the server a dialect names installs the
// extension called name in every database it creates. The name is compared
// exactly, as the catalog reports it. A dialect with no such extensions
// answers false for every name.
//
// PostgreSQL's plpgsql is not answered here: a PostgreSQL user may drop it, and
// the comparison leaves it alone through the default ignore list, which a
// caller can clear.
func IsExtension(dialect, name string) bool {
	return platform.NormalizeDialect(dialect) == platform.YugabyteDB && slices.Contains(yugabyteDBExtensions, name)
}
