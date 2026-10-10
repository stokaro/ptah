package catalog

import (
	"maps"
	"slices"
)

// Clone returns an index with independent mutable fields. Facets already own
// immutable values, so their shared representation cannot change either copy.
func (i Index) Clone() Index {
	i.Columns = slices.Clone(i.Columns)
	i.Parts = slices.Clone(i.Parts)
	i.IncludeColumns = slices.Clone(i.IncludeColumns)
	i.StorageParams = maps.Clone(i.StorageParams)
	i.RequiresExtensions = slices.Clone(i.RequiresExtensions)
	if i.NullsDistinct != nil {
		i.NullsDistinct = new(*i.NullsDistinct)
	}
	return i
}
