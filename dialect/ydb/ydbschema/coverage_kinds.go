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
	// A document in a format with no spelling for a TTL, DBML, records the
	// whole kind; see [CoverageFormatLimits].
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
// YDB coverage kind, in a new slice. HCL and DBML cannot spell a changefeed or
// a column-oriented table, and DBML cannot spell a TTL either. A document in
// one of them records those kinds as not described, with the format as the
// reason.
//
// The records explain the document's silence. They do not keep the objects:
// HCL and DBML do not claim to describe every changefeed and TTL the owner
// models, so the comparison leaves the ones the database holds and the
// document does not name as they are. Measured on
// YDB 26.2.1.14 and 25.1.4.7 with no format limit stated, an HCL document
// applied to a table carrying a changefeed planned nothing, a rebuild it
// asked for dropped the changefeed and added it back with its consumer, and
// a document that names no TTL kept the table's TTL.
//
// HCL carries a TTL as the owner's `platform "ydb"` properties, so it records
// no TTL. Measured on YDB 26.2.1.14, an HCL document with those properties
// created the table with its TTL, `ptah-compat schema inspect` wrote the
// properties back, the inspected document applied back planned nothing, and
// a document with another interval planned `ALTER TABLE ... SET (TTL = ...)`.
func CoverageFormatLimits() []coverage.FormatLimit {
	return []coverage.FormatLimit{
		{Format: coverage.FormatHCL, Kinds: []coverage.Kind{CoverageChangefeed, CoverageColumnTable}},
		{Format: coverage.FormatDBML, Kinds: []coverage.Kind{CoverageChangefeed, CoverageColumnTable, CoverageTTL}},
	}
}
