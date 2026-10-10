package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
)

// A YDB async replication or transfer is selected on its own name, in the
// directory that holds it, each by a type of its own.
func (s *scopeSelection) selectReplicationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = s.selectNamedType(objects, coverage, ydbreplication.ReplicationKind, "async_replication")
	return s.selectNamedType(objects, coverage, ydbreplication.TransferKind, "transfer")
}

// filterReplicationFeatures drops YDB async replications and transfers an
// exclusion selector names, and the ones whose directory is excluded, on both
// sides of a comparison.
func (s *exclusionState) filterReplicationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = s.filterNamedType(objects, coverage, ydbreplication.ReplicationKind, "async_replication")
	return s.filterNamedType(objects, coverage, ydbreplication.TransferKind, "transfer")
}
