package schemafingerprint

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// Only facet slots are cleared during fingerprinting. Clone their containing
// slices, including nested catalog columns, so clearing them cannot change the
// source. Other common fields are read by encoding/json without mutation.
func cloneObservedEnvelopes(source *catalog.Database) catalog.Database {
	result := *source
	result.Schemas = slices.Clone(source.Schemas)
	result.Tables = slices.Clone(source.Tables)
	result.Indexes = slices.Clone(source.Indexes)
	result.Constraints = slices.Clone(source.Constraints)
	result.Enums = slices.Clone(source.Enums)
	result.Domains = slices.Clone(source.Domains)
	result.Composites = slices.Clone(source.Composites)
	result.Ranges = slices.Clone(source.Ranges)
	result.Functions = slices.Clone(source.Functions)
	result.Sequences = slices.Clone(source.Sequences)
	result.Views = slices.Clone(source.Views)
	result.MatViews = slices.Clone(source.MatViews)
	result.Triggers = slices.Clone(source.Triggers)
	result.Roles = slices.Clone(source.Roles)
	for i := range result.Tables {
		result.Tables[i].Columns = slices.Clone(source.Tables[i].Columns)
	}
	return result
}

func cloneDesiredEnvelopes(source *schemamodel.Database) schemamodel.Database {
	result := *source
	result.Schemas = slices.Clone(source.Schemas)
	result.Tables = slices.Clone(source.Tables)
	result.Fields = slices.Clone(source.Fields)
	result.Indexes = slices.Clone(source.Indexes)
	result.Constraints = slices.Clone(source.Constraints)
	result.Enums = slices.Clone(source.Enums)
	result.Domains = slices.Clone(source.Domains)
	result.CompositeTypes = slices.Clone(source.CompositeTypes)
	result.Ranges = slices.Clone(source.Ranges)
	result.Functions = slices.Clone(source.Functions)
	result.Sequences = slices.Clone(source.Sequences)
	result.Views = slices.Clone(source.Views)
	result.MaterializedViews = slices.Clone(source.MaterializedViews)
	result.Triggers = slices.Clone(source.Triggers)
	result.Roles = slices.Clone(source.Roles)
	return result
}
