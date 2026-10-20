package difftypes

import (
	"encoding/json"
	"slices"

	"ptah.run/core/schemacapture"
)

// TableRemoval carries the observed state that a table drop would destroy.
// The capture includes common children, feature objects, facets, and coverage.
// Missing captured state does not establish an empty feature namespace.
type TableRemoval struct {
	// Name is the target-specific spelling used by the comparison and report.
	Name string
	// Current is an independent snapshot of the table before the removal.
	Current schemacapture.TableObservation
}

// Clone returns an independent copy of the removal's captured state.
func (r TableRemoval) Clone() TableRemoval {
	r.Current = r.Current.Clone()
	return r
}

// TableRemovals holds ordered table drops and their observed operands.
type TableRemovals []TableRemoval

// Clone copies all removal operands, including their nested common children.
func (r TableRemovals) Clone() TableRemovals {
	result := slices.Clone(r)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
}

// Names returns the report spellings without exposing the collection's storage.
func (r TableRemovals) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, len(r))
	for i, removal := range r {
		names[i] = removal.Name
	}
	return names
}

// MarshalJSON writes the removal names for a diff report. A report is not a
// replayable plan and does not encode the captured planning operands.
func (r TableRemovals) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Names())
}
