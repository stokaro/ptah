package featurereport

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// TableOwners resolves a feature object's parent against the supplied common
// tables. It requires an exact source match: guessing identifier folding or a
// default schema could silently assign a child to a different table. Reporting
// follows captured ownership; it does not decide database name equivalence.
type TableOwners struct {
	source map[tableSource][]int
}

// tableSource retains component boundaries while matching source provenance.
// It does not establish database identity or normalize a name.
type tableSource struct{ schema, name string }

// NewTableOwners indexes source tables without interpreting feature payloads.
func NewTableOwners(tables []schemamodel.Table) TableOwners {
	result := TableOwners{
		source: make(map[tableSource][]int, len(tables)),
	}
	for i, table := range tables {
		key := tableSource{table.Schema, table.Name}
		result.source[key] = append(result.source[key], i)
	}
	return result
}

// Resolve returns the original table position, or -1 for a standalone object.
// A missing or ambiguous parent is an error, never proof of an excluded table.
func (o TableOwners) Resolve(ref objectidentity.ID) (int, error) {
	if ref.Parent.Empty() {
		return -1, nil
	}
	var matches []int
	schema := ref.Schema.Source
	if ref.Schema.Defaulted {
		schema = ""
	}
	if ref.Catalog.Empty() && ref.Parent.Source != "" {
		matches = o.source[tableSource{schema, ref.Parent.Source}]
	}
	if len(matches) != 1 {
		return -1, fmt.Errorf("%w: feature object %s resolves to %d parent tables", schemaext.ErrInvalidValue, ref, len(matches))
	}
	return matches[0], nil
}
