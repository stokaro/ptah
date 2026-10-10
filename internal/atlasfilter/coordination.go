package atlasfilter

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
)

// namedFeatureKinds are the standalone feature kinds a scope selects by their
// own type and name, each through [selectNamedFeatures]. The tables one of
// them binds decide only whether a scope splits it; see
// [featureselect.Bindings.Select]. A kind selected by name is listed here in
// the same change that adds its selector.
var namedFeatureKinds = []schemaext.Kind{
	ydbcoordination.Kind, ydbsecret.Kind, ydbstreaming.Kind, ydbtopic.Kind, tsschema.ContinuousAggregateKind,
	ydbworkload.PoolKind, ydbworkload.ClassifierKind, ydbreplication.ReplicationKind, ydbreplication.TransferKind,
}

// Coordination objects are standalone. Table selection must not remove them,
// and the existing coordination_node selectors apply to values and knowledge.
// keepNode receives the schema the source wrote, so an unqualified object is
// resolved under the filter's own default schema rather than under the one
// its identity was built with.
func selectNamedFeatures(objects schemaext.Objects, coverage schemaext.Coverage, kind schemaext.Kind, keepNode func(schema, name string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(kind) || keepNode(ref.Schema.Authored(), ref.Name.Source)
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}

// selectNamedType keeps the features of kind that the scope's selectors name
// under typeName, the Atlas type a selector uses for them.
func (s *scopeSelection) selectNamedType(objects schemaext.Objects, coverage schemaext.Coverage, kind schemaext.Kind, typeName string) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, kind, func(schema, name string) bool {
		return s.selected(typeList(typeName), schema, name)
	})
}

// filterNamedType drops the features of kind that an exclusion selector names
// under typeName, and the ones whose directory is excluded, on both sides of a
// comparison.
func (s *exclusionState) filterNamedType(objects schemaext.Objects, coverage schemaext.Coverage, kind schemaext.Kind, typeName string) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, kind, func(schema, name string) bool {
		return !s.matches(typeName, s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}

func (s *scopeSelection) selectCoordinationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbcoordination.Kind, func(schema, name string) bool {
		return s.selected(typeList("coordination_node"), schema, name)
	})
}

func (s *exclusionState) filterCoordinationFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbcoordination.Kind, func(schema, name string) bool {
		return !s.matches("coordination_node", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
