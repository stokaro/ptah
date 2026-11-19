// Package mysqlpartition refuses a MySQL or MariaDB table's partitioning, and
// words the note a read gives for a table partitioned outside Ptah.
//
// Ptah neither reads nor renders MySQL partitioning (stokaro/ptah#6357), so a
// declaration that carries it would be created as one ordinary table, or
// compared as synced with an existing table that has none. Each source that
// can state it refuses it with [Refusal]: the MySQL owner's SQL syntax in a
// MySQL or MariaDB document, CREATE TABLE and ALTER TABLE alike, its table
// properties in a Go or YAML model, the PostgreSQL partition model rendered
// for MySQL or MariaDB, and the MySQL renderer for a clause read with no
// target.
//
// A table the database holds partitioned is read without its partitions. The
// read does not refuse it, since a change Ptah plans for the table keeps them,
// and names the table with [ReadNote] instead.
package mysqlpartition

import (
	"fmt"

	"ptah.run/core/ptaherr"
)

// Option is the table option a PARTITION BY clause is held under once it is
// read as an option: in a document read with no target, and as the
// partition_by platform property of a Go model.
const Option = "PARTITION_BY"

// Property is the platform property of the MySQL owner a model would state
// partitioning with.
const Property = "partition_by"

// NotModeled says that Ptah does not model the partitioning, and names the
// issue that tracks it. Each refusal and each read note carries it.
const NotModeled = "Ptah does not model MySQL or MariaDB partitioning (stokaro/ptah#6357)"

// ReadNote is why a read that found a partitioned table compares it without
// its partitions, as a report prints it after the table's name.
const ReadNote = NotModeled + ", so no comparison sees the partitions, and SQL written from this read creates the table without them"

// Refusal is the error for the partitioning subject names, such as
// "PARTITION BY on table sales". It wraps [ptaherr.ErrUnsupportedFeature].
func Refusal(subject string) error {
	return fmt.Errorf("%w: %s: %s; leave it out of the desired schema and manage the partitions outside Ptah",
		ptaherr.ErrUnsupportedFeature, subject, NotModeled)
}
