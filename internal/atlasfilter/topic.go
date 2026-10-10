package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
)

// A YDB topic is selected on its own name, in the directory that holds it.
// Its consumers belong to it and are never selected on their own.
func (s *scopeSelection) selectTopicFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbtopic.Kind, func(schema, name string) bool {
		return s.selected(typeList("topic"), schema, name)
	})
}

// filterTopicFeatures drops YDB topics an exclusion selector names, and topics
// whose directory is excluded, on both sides of a comparison.
func (s *exclusionState) filterTopicFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbtopic.Kind, func(schema, name string) bool {
		return !s.matches("topic", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
