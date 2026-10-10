package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
)

// A YDB async replication or transfer is selected on its own name, in the
// directory that holds it, each by a type of its own.
func (s *scopeSelection) selectReplicationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbreplication.ReplicationKind, func(schema, name string) bool {
		return s.selected(typeList("async_replication"), schema, name)
	})
	return selectNamedFeatures(objects, coverage, ydbreplication.TransferKind, func(schema, name string) bool {
		return s.selected(typeList("transfer"), schema, name)
	})
}

// filterReplicationFeatures drops YDB async replications and transfers an
// exclusion selector names, and the ones whose directory is excluded, on both
// sides of a comparison.
func (s *exclusionState) filterReplicationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbreplication.ReplicationKind, func(schema, name string) bool {
		return !s.matches("async_replication", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
	return selectNamedFeatures(objects, coverage, ydbreplication.TransferKind, func(schema, name string) bool {
		return !s.matches("transfer", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
