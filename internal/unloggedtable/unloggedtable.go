// Package unloggedtable answers which targets have an unlogged table, for the
// renderer that writes the keyword and the reader that looks for it back.
//
// It is one predicate rather than one per caller because the two ends have to
// agree: a renderer that emits CREATE UNLOGGED TABLE for a target whose reader
// never projects relpersistence describes the table as logged on the next read,
// and the comparator then proposes the same change on every run. Splitting the
// set is how the two stop agreeing, and neither end alone can see it.
package unloggedtable

import "ptah.run/core/platform"

// Supported reports whether a target creates a table whose writes skip the
// write-ahead log.
//
// It is narrower than platform.IsPostgresFamily, which also holds CockroachDB
// and Spanner. Neither creates one, and neither is asked for pg_class
// relpersistence: a projection a catalog does not carry fails the whole table
// read rather than the one column, so the reader asks only where an answer can
// be true.
//
// An unrecognized or empty dialect answers false.
func Supported(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.YugabyteDB:
		return true
	default:
		return false
	}
}
