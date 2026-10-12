// Package unloggedtable answers which targets have an unlogged table, for the
// renderer that writes the keyword, the reader that looks for it back, and the
// comparison that plans a change between logged and unlogged.
//
// It is one predicate rather than one per caller because the three have to
// agree: a renderer that emits CREATE UNLOGGED TABLE for a target whose reader
// never projects relpersistence describes the table as logged on the next read,
// and the comparison then proposes the same change on every run. Splitting the
// set is how they stop agreeing, and no one of them alone can see it.
package unloggedtable

import "ptah.run/core/platform"

// Supported reports whether a target creates a table whose writes skip the
// write-ahead log.
//
// It is narrower than platform.IsPostgresFamily, which also holds
// CockroachDB, YugabyteDB and Spanner. None of them creates one. CockroachDB
// and Spanner are not asked for pg_class relpersistence: a projection a catalog
// does not carry fails the whole table read rather than the one column, so the
// reader asks only where an answer can be true. YugabyteDB accepts the keyword
// and creates an ordinary table: measured on 2026.1.2, CREATE UNLOGGED TABLE
// succeeds and pg_class reports relpersistence 'p', so a comparison that read
// the keyword there would plan SET UNLOGGED on every run.
//
// An unrecognized or empty dialect answers false.
func Supported(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.Postgres
}
