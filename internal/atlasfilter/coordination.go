package atlasfilter

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// Coordination objects are standalone. Table selection must not remove them,
// and the existing coordination_node selectors apply to values and knowledge.
func selectCoordinationFeatures(objects schemaext.Objects, coverage schemaext.Coverage, keepNode func(schema, name string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(ydbcoordination.Kind) || keepNode(ref.Schema.Source, ref.Name.Source)
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}

func (s *scopeSelection) selectCoordinationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectCoordinationFeatures(objects, coverage, func(schema, name string) bool {
		return s.selected(typeList("coordination_node"), schema, name)
	})
}

func (s *exclusionState) filterCoordinationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectCoordinationFeatures(objects, coverage, func(schema, name string) bool {
		return !s.matches("coordination_node", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
