package schemadiff

import (
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// A common schema exclusion also limits absence claims for named features.
// Project it onto the subjects this comparison actually asks about; retain
// existing values and stronger subject limits, and never enroll a new model.
func namedFeatureSchemaLimits(source, peer schemaext.FeatureState, limits coverage.Set) (schemaext.Coverage, error) {
	if limits.IsZero() || source.Coverage.IsZero() {
		return source.Coverage, nil
	}
	kinds := source.Coverage.KindRecords()
	enrolled := make(map[schemaext.Kind]bool, len(kinds))
	for _, record := range kinds {
		enrolled[record.Model.Kind] = true
	}
	known := make(map[objectidentity.Key]bool)
	for _, ref := range source.Objects.Refs() {
		known[ref.Key()] = true
	}
	subjects := source.Coverage.SubjectRecords()
	for _, ref := range peer.Objects.Refs() {
		kind := schemaext.Kind(ref.Kind)
		if known[ref.Key()] || !enrolled[kind] || limits.DescribesSchema(ref.Schema.Source) {
			continue
		}
		if _, found := source.Coverage.SubjectKnowledge(kind, ref); found {
			continue
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: kind, Subject: ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source did not describe this object's schema"}})
	}
	return schemaext.NewCoverage(source.Coverage.Representation(), kinds, subjects)
}
