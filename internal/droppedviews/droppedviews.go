// Package droppedviews splits the views and materialized views a schema diff
// removes into the ones an addition of the other kind replaces under the same
// name and the ones the plan drops for good.
//
// A planner orders the two apart: ClickHouse refuses to create a view while
// a materialized view still holds its name, and the other way round, so a
// replacement's drop runs before the creation that takes the name, and a drop
// for good runs with the other removals. The comparison reads the second set
// too. A target that keeps an object past its relation, as ClickHouse keeps a
// row policy bound to a name, drops such an object together with a relation
// whose name the plan leaves to no view ([Vacated]), so the comparison does
// not report the object undecided (see
// [ptah.run/engine.ObjectComparison.OutlivesParent]). The planner and the
// comparison read this one answer, so the relations one drops objects with
// are the relations the other stops reporting.
package droppedviews

import (
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/objectlookup"
)

// Removal is one view or materialized view a diff removes, by the name the
// diff gives it.
type Removal struct {
	Name         string
	Materialized bool
}

// Split returns the removals of diff an addition of the other kind replaces,
// and the others, which the plan drops for good. Replaced holds the
// materialized views first and then the views; gone holds the views first
// and then the materialized views; each keeps the diff's order within a
// kind. The two sides of a diff come from different schemas and need not
// spell a name alike, so a name matches under [objectlookup.Contains], never
// by string equality. A view removed while a table of its name is created is
// gone: no view takes its name.
func Split(diff *difftypes.SchemaDiff, semantics identifier.Semantics) (replaced, gone []Removal) {
	if diff == nil {
		return nil, nil
	}
	viewsAdded, materializedAdded := diff.ViewsAdded.Names(), diff.MaterializedViewsAdded.Names()
	var goneMaterialized []Removal
	for _, name := range diff.MaterializedViewsRemoved.Names() {
		removal := Removal{Name: name, Materialized: true}
		if objectlookup.Contains(viewsAdded, name, semantics) {
			replaced = append(replaced, removal)
			continue
		}
		goneMaterialized = append(goneMaterialized, removal)
	}
	for _, name := range diff.ViewsRemoved.Names() {
		removal := Removal{Name: name}
		if objectlookup.Contains(materializedAdded, name, semantics) {
			replaced = append(replaced, removal)
			continue
		}
		gone = append(gone, removal)
	}
	return replaced, append(gone, goneMaterialized...)
}

// Vacated returns the removals of diff that leave their name to no view or
// materialized view: the ones dropped for good that no addition of either
// kind takes back, in the order [Split] gives them. A target that keeps an
// object bound to a relation's name drops it with these relations and keeps
// it with every other, since a relation of the same name rebuilt in its place
// takes the object over.
func Vacated(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []Removal {
	_, gone := Split(diff, semantics)
	if len(gone) == 0 {
		return nil
	}
	added := append(diff.ViewsAdded.Names(), diff.MaterializedViewsAdded.Names()...)
	var vacated []Removal
	for _, removal := range gone {
		if !objectlookup.Contains(added, removal.Name, semantics) {
			vacated = append(vacated, removal)
		}
	}
	return vacated
}

// Holds reports whether one of removals is the relation name refers to,
// under the rule [Split] matches names by.
func Holds(removals []Removal, name string, semantics identifier.Semantics) bool {
	names := make([]string, len(removals))
	for i, removal := range removals {
		names[i] = removal.Name
	}
	return objectlookup.Contains(names, name, semantics)
}
