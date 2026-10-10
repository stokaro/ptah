package atlasfilter

import (
	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
)

// filterSynonymFeatures drops the synonyms an exclusion selector names, and
// the ones whose own schema is excluded. An excluded synonym's name is
// excluded as a table name too, as every relation-like name is.
//
// The selector matches the ALIAS, never the target. A synonym excluded because
// its target was excluded would be a different rule: the alias is the object
// this schema declares, and the target may not even be in this database.
func (s *exclusionState) filterSynonymFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, synonym.Kind, func(schema, name string) bool {
		if s.matches("synonym", s.nameCandidates(schema, name)...) || s.schemaExcluded(schema) {
			s.excludeTable(schema, name)
			return false
		}
		return true
	})
}

// selectSynonymFeatures keeps the synonyms a scope selects on their own name,
// never on their target's. The alias is the object this schema declares, and
// a rule that kept a synonym because its target was selected would pull in an
// alias the operator did not name, possibly one whose target is not even in
// this database.
func (s *scopeSelection) selectSynonymFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return s.selectNamedType(objects, coverage, synonym.Kind, "synonym")
}

// selectGeneratedSynonymFeatures is [scopeSelection.selectSynonymFeatures]
// for a declared schema, whose names are matched qualified.
func (s *scopeSelection) selectGeneratedSynonymFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, synonym.Kind, func(schema, name string) bool {
		return s.selectedQualifiedName(typeList("synonym"), synonym.Synonym{Schema: schema, Name: name}.QualifiedName())
	})
}
