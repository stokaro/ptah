package engine

import (
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Whole-schema validation and rendering need codecs for concrete declarations.
// A model represented only by deliberately excluded facet bindings contributes
// no payload to either operation. Validate the remaining model identities while
// preserving the original source coverage in the schema passed to the service:
// comparison still needs those claims for owners without an exclusion.
func declaredModelCoverage(schema *schemamodel.Database) schemaext.Coverage {
	excluded := make(map[schemaext.Kind]bool)
	active := make(map[schemaext.Kind]bool)
	for _, facets := range schema.FacetSlots() {
		for _, kind := range facets.DeclaredKinds() {
			excluded[kind] = true
		}
		for _, kind := range facets.Kinds() {
			active[kind] = true
		}
	}
	for _, ref := range schema.FeatureObjects.Refs() {
		active[schemaext.Kind(ref.Kind)] = true
	}
	var kinds []schemaext.Kind
	for _, record := range schema.FeatureCoverage.KindRecords() {
		kind := record.Model.Kind
		if !excluded[kind] || active[kind] {
			kinds = append(kinds, kind)
		}
	}
	return schema.FeatureCoverage.SelectKinds(kinds)
}
