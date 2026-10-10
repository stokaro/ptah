// Package facetsplit takes the table and index facets an exporter writes
// itself out of a schema before the platform property encoder sees it, and
// puts them back afterwards.
//
// [ptah.run/core/schemaproperties.EncodeTables] refuses a whole schema when one table facet
// has no property spelling. An exporter that writes some facets in a form of
// its own -- a Go annotation, an HCL block, a loss it reports by name -- sets
// those aside first, so they do not keep every other facet from becoming
// platform properties.
package facetsplit

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// SetAside returns a copy of db whose tables keep only the facets own
// rejects, and the facets it took from each table, in table order. db is not
// changed.
func SetAside(db *schemamodel.Database, own func(schemaext.Kind) bool) (*schemamodel.Database, []schemaext.Facets) {
	result := *db
	result.Tables = slices.Clone(db.Tables)
	aside := make([]schemaext.Facets, len(result.Tables))
	for i := range result.Tables {
		result.Tables[i].Facets, aside[i] = split(result.Tables[i].Facets, own)
	}
	return &result, aside
}

// Restore returns a copy of db with the facets [SetAside] took put back on
// their tables, which the property encoder keeps in order. A table count that
// changed in between is an error wrapping schemaext.ErrInvalidValue. db is not
// changed.
func Restore(db *schemamodel.Database, aside []schemaext.Facets) (*schemamodel.Database, error) {
	if len(db.Tables) != len(aside) {
		return nil, fmt.Errorf("%w: table property encoder changed the table count", schemaext.ErrInvalidValue)
	}
	result := *db
	result.Tables = slices.Clone(db.Tables)
	for i := range result.Tables {
		merged, err := result.Tables[i].Facets.Merge(aside[i])
		if err != nil {
			return nil, err
		}
		result.Tables[i].Facets = merged
	}
	return &result, nil
}

// SetAsideIndexes is [SetAside] for index facets: it returns a copy of db
// whose indexes keep only the facets own rejects, and the facets it took
// from each index, in index order. db is not changed.
func SetAsideIndexes(db *schemamodel.Database, own func(schemaext.Kind) bool) (*schemamodel.Database, []schemaext.Facets) {
	result := *db
	result.Indexes = slices.Clone(db.Indexes)
	aside := make([]schemaext.Facets, len(result.Indexes))
	for i := range result.Indexes {
		result.Indexes[i].Facets, aside[i] = split(result.Indexes[i].Facets, own)
	}
	return &result, aside
}

// RestoreIndexes is [Restore] for the facets [SetAsideIndexes] took. An index
// count that changed in between is an error wrapping
// schemaext.ErrInvalidValue. db is not changed.
func RestoreIndexes(db *schemamodel.Database, aside []schemaext.Facets) (*schemamodel.Database, error) {
	if len(db.Indexes) != len(aside) {
		return nil, fmt.Errorf("%w: index property encoder changed the index count", schemaext.ErrInvalidValue)
	}
	result := *db
	result.Indexes = slices.Clone(db.Indexes)
	for i := range result.Indexes {
		merged, err := result.Indexes[i].Facets.Merge(aside[i])
		if err != nil {
			return nil, err
		}
		result.Indexes[i].Facets = merged
	}
	return &result, nil
}

// split returns the facets own rejects and the facets it takes.
func split(facets schemaext.Facets, own func(schemaext.Kind) bool) (kept, taken schemaext.Facets) {
	kept, taken = facets, facets
	for _, kind := range facets.DeclaredKinds() {
		if own(kind) {
			kept = kept.Without(kind)
			continue
		}
		taken = taken.Without(kind)
	}
	return kept, taken
}
