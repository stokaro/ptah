package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// A continuous aggregate is selected on its own name, in the schema that holds
// it, with the continuous_aggregate selector. Hypertable settings need no
// selector of their own: they are a facet of their table, so table selection
// keeps or drops them with it.
func (s *scopeSelection) selectTimescaleFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, tsschema.ContinuousAggregateKind, func(schema, name string) bool {
		return s.selected(typeList("continuous_aggregate"), schema, name)
	})
}

// Excluding an aggregate is meaningful even though a narrowed comparison then
// leaves it alone: an operator who excludes it is saying the comparison should
// neither drop it nor refuse a declaration over it. The object stays on the
// server either way.
func (s *exclusionState) filterTimescaleFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, tsschema.ContinuousAggregateKind, func(schema, name string) bool {
		return !s.matches("continuous_aggregate", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
