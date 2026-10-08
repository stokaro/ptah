// Package featurereport collects common schema extension envelopes for reporting.
// Concrete feature values are interpreted only by the selected runtime.
package featurereport

import (
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Values collects named objects and attached facets without assuming a feature
// vocabulary. Ordering is deterministic and duplicate facets on different
// owners remain separate values. The returned values are independent snapshots.
func Values(db *schemamodel.Database) ([]schemaext.Value, error) {
	if db == nil {
		return nil, nil
	}
	objects, err := db.FeatureObjects.All()
	if err != nil {
		return nil, err
	}
	values := make([]schemaext.Value, 0, len(objects))
	for _, object := range objects {
		values = append(values, object.Value)
	}
	for _, facets := range db.FacetSlots() {
		attached, err := facets.Values()
		if err != nil {
			return nil, err
		}
		values = append(values, attached...)
	}
	return values, nil
}
