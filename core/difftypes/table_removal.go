package difftypes

import (
	"encoding/json"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
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

// ObservedReferences returns, for each removal whose captured foreign keys
// reference another table of the collection, the names of those tables, in
// the order the keys were captured and each once. A table matches a reference
// by semantics; a reference that names no schema means the referencing
// table's schema. A key that references the table it is on, or a table the
// collection does not remove, contributes nothing. The result is keyed by
// [TableRemoval.Name], the names [TableRemovals.Names] returns, so a planner
// can order the drops a declaration that no longer names these tables cannot
// order.
func (r TableRemovals) ObservedReferences(semantics identifier.Semantics) map[string][]string {
	builder := objectidentity.NewBuilder(semantics)
	names := make(map[objectidentity.Key]string, len(r))
	for _, removal := range r {
		if removal.Current.HasTable() {
			names[builder.TableParts(removal.Current.Table.Schema, removal.Current.Table.Name).Key()] = removal.Name
		}
	}
	var references map[string][]string
	for _, removal := range r {
		for _, constraint := range removal.Current.Constraints {
			if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || constraint.ForeignTable == nil {
				continue
			}
			schema := constraint.ForeignSchema
			if schema == "" {
				schema = removal.Current.Table.Schema
			}
			referenced, removed := names[builder.TableParts(schema, *constraint.ForeignTable).Key()]
			if !removed || referenced == removal.Name || slices.Contains(references[removal.Name], referenced) {
				continue
			}
			if references == nil {
				references = make(map[string][]string)
			}
			references[removal.Name] = append(references[removal.Name], referenced)
		}
	}
	return references
}

// MarshalJSON writes the removal names for a diff report. A report is not a
// replayable plan and does not encode the captured planning operands.
func (r TableRemovals) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Names())
}
