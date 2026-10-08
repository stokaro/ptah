package catalog

import "ptah.run/core/schemaext"

// FacetSlots enumerates the facet collections attached to common envelopes.
// Each pointer addresses the mutable database passed by the caller. Replacing
// a slot changes that database; reading a collection retains snapshot ownership.
func (db *Database) FacetSlots() []*schemaext.Facets {
	result := []*schemaext.Facets{&db.Facets}
	for i := range db.Schemas {
		result = append(result, &db.Schemas[i].Facets)
	}
	for i := range db.Tables {
		result = append(result, &db.Tables[i].Facets)
		for j := range db.Tables[i].Columns {
			result = append(result, &db.Tables[i].Columns[j].Facets)
		}
	}
	for i := range db.Indexes {
		result = append(result, &db.Indexes[i].Facets)
	}
	for i := range db.Constraints {
		result = append(result, &db.Constraints[i].Facets)
	}
	for i := range db.Enums {
		result = append(result, &db.Enums[i].Facets)
	}
	for i := range db.Domains {
		result = append(result, &db.Domains[i].Facets)
	}
	for i := range db.Composites {
		result = append(result, &db.Composites[i].Facets)
	}
	for i := range db.Ranges {
		result = append(result, &db.Ranges[i].Facets)
	}
	for i := range db.Functions {
		result = append(result, &db.Functions[i].Facets)
	}
	for i := range db.Sequences {
		result = append(result, &db.Sequences[i].Facets)
	}
	for i := range db.Views {
		result = append(result, &db.Views[i].Facets)
	}
	for i := range db.MatViews {
		result = append(result, &db.MatViews[i].Facets)
	}
	for i := range db.Triggers {
		result = append(result, &db.Triggers[i].Facets)
	}
	for i := range db.Roles {
		result = append(result, &db.Roles[i].Facets)
	}
	return result
}
