package schemamodel

import (
	"maps"
	"slices"
)

// Clone returns an index whose mutable fields are independent of the original.
// Facets retain immutable value ownership in both copies.
func (i Index) Clone() Index {
	i.Fields = slices.Clone(i.Fields)
	i.Parts = slices.Clone(i.Parts)
	i.IncludeColumns = slices.Clone(i.IncludeColumns)
	i.StorageParams = maps.Clone(i.StorageParams)
	i.Overrides = cloneOverrides(i.Overrides)
	i.RequiresExtensions = slices.Clone(i.RequiresExtensions)
	i.Partitioning = i.Partitioning.Clone()
	i.Vector = i.Vector.Clone()
	if i.NullsDistinct != nil {
		i.NullsDistinct = new(*i.NullsDistinct)
	}
	return i
}
