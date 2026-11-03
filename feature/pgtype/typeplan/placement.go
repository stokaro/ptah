package typeplan

import (
	"cmp"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/deporder"
	"ptah.run/internal/typeorder"
	"ptah.run/internal/usertypescope"
)

// placement is where the plan puts each type against the relations the
// common steps create and drop.
//
// A domain's base type, a composite type's attribute or a range type's
// subtype may be a relation's row type: `CREATE TYPE wrap AS (r t, n int)`.
// PostgreSQL then records a dependency on the relation, so the type is
// created after it and dropped before it. The type phase cannot do either: its
// creation window lies before the tables and its removal window after them.
// Such a type joins the default phase instead, whose creation window lies
// after the tables and columns and before the views, and whose removal window
// lies after the views and columns are dropped and before the tables. An edge
// from the relation's step holds the creation after it, and one to the
// table's step holds the drop before it, which a whole-schema render, with no
// windows of its own, orders by.
//
// A type that names no relation the plan creates or drops keeps the type
// phase.
type placement struct {
	// after maps a type to the relation creations its creation, change in
	// place or creation comment waits for.
	after map[string][]typeorder.RelationStep
	// before maps a type the plan removes to the table drops its drop goes
	// ahead of. A type something dropped after the tables still uses is not
	// in it; see [typeorder.DroppedBeforeTables].
	before map[string][]typeorder.RelationStep
}

// placeChanges places changes against the common steps, with the type
// definitions types holds beside them. Names resolve as semantics says.
func placeChanges(changes []*change, types featureplan.TypeContext, common []featureplan.CommonStep, semantics identifier.Semantics) placement {
	var desired, current []deporder.UserType
	var removed []string
	for _, planned := range changes {
		if planned.after {
			desired = append(desired, deporder.UserType{Name: planned.name, References: planned.declared})
		}
		if planned.before {
			current = append(current, deporder.UserType{Name: planned.name, References: planned.held})
		}
		if !planned.after {
			removed = append(removed, planned.name)
		}
	}
	return placement{
		after:  typeorder.CreatedAfterRelations(typeorder.Vocabulary(desired, types.Desired), common, semantics),
		before: typeorder.DroppedBeforeTables(typeorder.Vocabulary(current, types.Current), removed, common, semantics),
	}
}

// key is the name placement keys a change by.
func key(planned *change) string { return deporder.NormalizeTypeReference(planned.name) }

// relationsAfter are the relation creations a change waits for. A change
// with no step in the creation window, such as a removal or a comment of a
// type that survives, waits for none.
func (p placement) relationsAfter(planned *change) []typeorder.RelationStep {
	if !planned.after || (planned.survives() && !planned.rebuilt && (!planned.changed || planned.alter == nil)) {
		return nil
	}
	return p.after[key(planned)]
}

// late reports a change that is created or changed after the relations.
func (p placement) late(planned *change) bool { return len(p.relationsAfter(planned)) > 0 }

// ahead reports a removal that drops the type before the tables.
func (p placement) ahead(planned *change) bool {
	return !planned.after && len(p.before[key(planned)]) > 0
}

// creationWindow is the sequence a change's creation, change in place and
// creation comment join.
func (p placement) creationWindow(plan *windows, planned *change) *sequence {
	if p.late(planned) {
		return &plan.late
	}
	return &plan.creation
}

// creationPhase is the phase of the steps creationWindow holds.
func (p placement) creationPhase(planned *change) featureplan.Phase {
	if p.late(planned) {
		return featureplan.PhaseDefault
	}
	return featureplan.PhaseType
}

// dependencies are the edges that hold each placed change on its side of the
// relations' steps. The changes have been planned.
func (p placement) dependencies(changes []*change) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for _, planned := range changes {
		if len(planned.steps) == 0 {
			continue
		}
		for _, relation := range p.relationsAfter(planned) {
			edges = append(edges, plangraph.Dependency{Before: relation.Step, After: planned.steps[0]})
		}
		if p.ahead(planned) {
			for _, relation := range p.before[key(planned)] {
				edges = append(edges, plangraph.Dependency{Before: planned.steps[0], After: relation.Step})
			}
		}
	}
	return edges
}

// refuse returns the first change, by name, that no window can hold beside
// the refusal, or nil:
//
//   - a type the plan drops and creates again, which it does before the
//     tables, that names a relation the plan creates;
//   - a type that names a view or a materialized view the plan creates, since
//     the window after the tables lies before the views;
//   - a type a common step that creates or changes an object reads, such as
//     a column of a table the plan creates, a column it adds or changes to
//     the type, or a routine whose signature names it. Each of those comes
//     before the window the type waits in. A step that only drops, such as
//     the DROP TABLE of a table whose column holds a type the plan changes in
//     place, runs after every creation and is no conflict.
//
// A plan in any of these orders would stop at the statement that names the
// object the server has not created yet.
func (p placement) refuse(changes []*change, common []featureplan.CommonStep) (*change, schemavalidation.Diagnostic) {
	readers := typeorder.ReadBy(common, createsOrChanges)
	ordered := slices.SortedStableFunc(slices.Values(changes), func(a, b *change) int { return cmp.Compare(a.name, b.name) })
	for _, planned := range ordered {
		relations := p.relationsAfter(planned)
		if len(relations) == 0 {
			continue
		}
		relation, reason := relations[0], ""
		view := slices.IndexFunc(relations, func(relation typeorder.RelationStep) bool { return relation.Relation.Kind != objectidentity.KindTable })
		readBy := readers[key(planned)]
		switch {
		case view >= 0:
			relation, reason = relations[view], "Ptah creates such a type after the tables and before the views"
		case planned.rebuilt:
			reason = "Ptah creates such a type after the tables, and drops and creates again before them a type it cannot change in place"
		case len(readBy) > 0:
			used := describe(usedBy(readBy[0]))
			reason = fmt.Sprintf("%s uses it, and Ptah creates such a type after the tables and columns, so it cannot exist when %s is created", used, used)
		default:
			continue
		}
		return planned, schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Object: planned.name,
			Feature: usertypescope.Statement(capabilityOf(planned)),
			Message: fmt.Sprintf("%s %s depends on the row type of %s, which the plan creates; %s", pgtype.Label(planned.subject), planned.name, describe(relation.Relation), reason)}
	}
	return nil, schemavalidation.Diagnostic{}
}

// createsOrChanges reports a step that creates or changes an object.
func createsOrChanges(step featureplan.CommonStep) bool {
	return slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
		return effect.Action == plangraph.Create || effect.Action == plangraph.Alter
	})
}

// usedBy is the object a reading step creates or changes: a column it adds
// or changes, the table it creates, or the routine.
func usedBy(step featureplan.CommonStep) objectidentity.ID {
	var used objectidentity.ID
	for _, effect := range step.Effects {
		switch {
		case effect.Action == plangraph.Read:
		case effect.Subject.Kind == objectidentity.KindColumn:
			return effect.Subject
		case used.Name.Source == "":
			used = effect.Subject
		}
	}
	return used
}

// describe names an object by its kind and the name a statement writes.
func describe(id objectidentity.ID) string {
	name := typeorder.Name(id)
	if id.Parent.Source != "" {
		name = typeorder.Name(objectidentity.ID{Schema: id.Schema, Name: id.Parent}) + "." + id.Name.Source
	}
	return string(id.Kind) + " " + name
}

// capabilityOf is the capability that gates creating the change's model.
func capabilityOf(planned *change) capability.Capability {
	switch planned.model {
	case pgtype.DomainKind:
		return capability.DomainTypes
	case pgtype.RangeKind:
		return capability.RangeTypes
	default:
		return capability.CompositeTypes
	}
}
