package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// A YDB secret is selected on its own name, in the directory that holds it.
func (s *scopeSelection) selectSecretFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbsecret.Kind, func(schema, name string) bool {
		return s.selected(typeList("secret"), schema, name)
	})
}

// filterSecretFeatures drops YDB secrets an exclusion selector names, and
// secrets whose directory is excluded, on both sides of a comparison.
func (s *exclusionState) filterSecretFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbsecret.Kind, func(schema, name string) bool {
		return !s.matches("secret", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}
