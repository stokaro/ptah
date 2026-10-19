package ydbschema

import "ptah.run/core/coverage"

// The YDB state a read records as not described rather than describes, as
// coverage kinds. A read records each one it meets, by the path of the object
// or of the table that carries it, so a description's silence about them is
// never read as their absence and nothing plans their removal.
const (
	// CoverageColumnTable is a column-oriented table (STORE = COLUMN), or the
	// column store that holds such tables.
	CoverageColumnTable coverage.Kind = "column_table"
	// CoverageReplicaTable is a table an async replication writes, named by
	// its path: read-only while the replication runs, and read-only for good
	// once the replication is dropped without being failed over first. YDB
	// marks one with the `__async_replica` attribute. A read records it rather
	// than describing it as a table, so no plan drops, changes or creates a
	// table at its path; the replication owns it.
	//
	// It is consulted in process rather than serialized: [CoverageKinds]
	// leaves it out and [ProcessCoverageKinds] names it, so no document can
	// name it, a writer leaves it out of one, and a set carrying it does not
	// survive a round trip through one. Hold the record in memory and consult
	// it there.
	CoverageReplicaTable coverage.Kind = "replica_table"
	// CoverageChangefeed is a changefeed, a stream of a table's changes. It is
	// named by the table's path and the changefeed's name.
	CoverageChangefeed coverage.Kind = "changefeed"
	// CoverageTTL is a table's time to live, or the part of it a description
	// does not describe. A read records, by the table's path, what a TTL
	// carries beyond the row deletion policy Ptah models: the run interval,
	// which only the SDK and the CLI set, and a column table's tiering policy.
	// A document in a format with no spelling for a TTL, HCL or DBML, records
	// the whole kind, and the comparison then keeps the policy the database
	// holds for each table, through a rebuild too.
	CoverageTTL coverage.Kind = "ttl"
	// CoverageTableOption is a table's storage settings Ptah does not model,
	// where they differ from what a new table is given: its tablet's commit
	// log pools, an external pool, external blobs. A read records them by the
	// table's path. A table's partitioning, read replicas and key bloom filter
	// are modeled, and a description that leaves them out, as HCL and DBML
	// always do, keeps what the table holds.
	CoverageTableOption coverage.Kind = "table_option"
)

// CoverageKinds returns the YDB coverage kinds a document may name, in a new
// slice. [CoverageReplicaTable] is not among them.
func CoverageKinds() []coverage.Kind {
	return []coverage.Kind{CoverageChangefeed, CoverageColumnTable, CoverageTableOption, CoverageTTL}
}

// ProcessCoverageKinds returns the YDB coverage kinds a read records for the
// owner to consult in the same process, which no document names, in a new
// slice: [CoverageReplicaTable].
func ProcessCoverageKinds() []coverage.Kind {
	return []coverage.Kind{CoverageReplicaTable}
}

// CoverageFormatLimits returns the document formats with no spelling for a
// YDB coverage kind, in a new slice. HCL and DBML cannot spell a changefeed,
// a column-oriented table or a TTL, so a document in either records the three
// kinds as not described, and applying it keeps what the database holds.
//
// HCL and DBML have no spelling for a table's row deletion policy or for a
// YDB changefeed, which a Go schema and a YAML schema have, and Spanner's SQL
// has for a policy. Measured on YDB 26.2.1.14, a TTL table inspected through
// `ptah-compat schema inspect` and applied back from that HCL planned
// `ALTER TABLE ... RESET (TTL)`, and an HCL or a DBML document declaring a
// table that carries a changefeed planned `DROP CHANGEFEED`.
func CoverageFormatLimits() []coverage.FormatLimit {
	return []coverage.FormatLimit{
		{Format: coverage.FormatHCL, Kinds: []coverage.Kind{CoverageChangefeed, CoverageColumnTable, CoverageTTL}},
		{Format: coverage.FormatDBML, Kinds: []coverage.Kind{CoverageChangefeed, CoverageColumnTable, CoverageTTL}},
	}
}
