package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// A YDB external data source or external table is selected on its own name,
// in the directory that holds it, each by a type of its own.
func (s *scopeSelection) selectExternalFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = s.selectNamedType(objects, coverage, ydbexternal.SourceKind, "external_data_source")
	return s.selectNamedType(objects, coverage, ydbexternal.TableKind, "external_table")
}

// filterExternalFeatures drops YDB external data sources and external tables
// an exclusion selector names, and the ones whose directory is excluded, on
// both sides of a comparison.
func (s *exclusionState) filterExternalFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = s.filterNamedType(objects, coverage, ydbexternal.SourceKind, "external_data_source")
	return s.filterNamedType(objects, coverage, ydbexternal.TableKind, "external_table")
}
