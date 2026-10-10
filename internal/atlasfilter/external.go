package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// A YDB external data source or external table is selected on its own name,
// in the directory that holds it, each by a type of its own.
func (s *scopeSelection) selectExternalFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbexternal.SourceKind, func(schema, name string) bool {
		return s.selected(typeList("external_data_source"), schema, name)
	})
	return selectNamedFeatures(objects, coverage, ydbexternal.TableKind, func(schema, name string) bool {
		return s.selected(typeList("external_table"), schema, name)
	})
}

// filterExternalFeatures drops YDB external data sources and external tables
// an exclusion selector names, and the ones whose directory is excluded, on
// both sides of a comparison.
func (s *exclusionState) filterExternalFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbexternal.SourceKind, func(schema, name string) bool {
		return !s.matches("external_data_source", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
	return selectNamedFeatures(objects, coverage, ydbexternal.TableKind, func(schema, name string) bool {
		return !s.matches("external_table", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
