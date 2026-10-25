package postgres

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
)

// refusePoliciesLostToCascade refuses a plan that drops a view, a materialized
// view or a routine a captured object depends on, such as a row-level security
// policy whose expression reads the relation or calls the routine.
//
// The server records that dependency. A drop with CASCADE takes the object too,
// and its owner plans no change for an object that did not change, so nothing
// would put it back: the plan applies, and the table has lost a control the
// schema still declares (stokaro/ptah#4305). A lost permissive policy hides the
// rows it admitted; a lost restrictive one reveals the rows it hid. A drop
// without CASCADE is refused by the server, so the plan could not apply
// (stokaro/ptah#4965). The plan is refused instead, and the error names the
// object and the change the drop is for. An object the plan removes too is not
// lost, so it is not a reason to refuse.
//
// A view rebuild drops a view or a materialized view with CASCADE in Ptah's own
// style. The exact style drops it without CASCADE unless the drop relies on
// it, which takes the policy as Ptah's own style does, so its error names both
// outcomes. A routine is dropped without CASCADE in both styles. A view the
// plan replaces in place is not dropped and is not a reason; see
// [viewRebuild.replaceFirst].
//
// The objects answer for themselves through [schemaext.RelationReader], so
// this planner names no owner. A diff built without a comparison carries no
// observation and is not checked.
func (p *Planner) refusePoliciesLostToCascade(diff *difftypes.SchemaDiff, rebuild viewRebuild) error {
	if diff.TablePreparation == nil {
		return nil
	}
	dropped := droppedReadables(diff, rebuild)
	if len(dropped) == 0 {
		return nil
	}
	for _, table := range diff.TablePreparation.Source {
		objects, err := table.Current.OwnedObjects.All()
		if err != nil {
			return err
		}
		for _, object := range objects {
			reader, ok := object.Value.(schemaext.RelationReader)
			if !ok || removedByPlan(table, object) {
				continue
			}
			for _, relation := range dropped {
				if !schemaext.ReadsRelationIn(reader, relation.name, diff.EffectiveIdentifierSemantics("")) {
					continue
				}
				return p.lostReaderRefusal(relation, fmt.Sprint(object.Ref))
			}
		}
	}
	return nil
}

// removedByPlan reports whether the plan removes object, a captured object of
// table: the declaration holds no object of that identity, so the object is
// not lost to a drop that takes it, and nothing needs to recreate it.
func removedByPlan(table schemapreparation.Table, object schemaext.Object) bool {
	key := object.Ref.Key()
	return !slices.ContainsFunc(table.Desired.OwnedObjects.Refs(), func(ref objectidentity.ID) bool {
		return ref.Key() == key
	})
}

// lostReaderRefusal is the refusal of a plan whose drop of relation would take
// the captured object reader with it, or stop on it.
func (p *Planner) lostReaderRefusal(relation droppedReadable, reader string) error {
	reads := "reads"
	if relation.kind == "routine" {
		reads = "calls"
	}
	outcome := p.lossOutcome(relation.kind)
	if relation.cause == nil {
		return fmt.Errorf("%w: the plan drops %s %s, which the declaration removes; %s %s it, and %s. "+
			"Change %s so it no longer %s %s first",
			ptaherr.ErrUnsupportedFeature, relation.kind, relation.name, reader, reads, outcome,
			reader, reads, relation.name)
	}
	return fmt.Errorf("%w: the plan drops %s %s and creates it again, for %s; %s %s it, and %s. "+
		"Change %s so it no longer %s %s in one migration, then %s in the next",
		ptaherr.ErrUnsupportedFeature, relation.kind, relation.name, relation.cause.change, reader, reads, outcome,
		reader, reads, relation.name, relation.cause.then)
}

// lossOutcome is what the drop of an object of kind does to a captured object
// that depends on it: a CASCADE takes it, and a drop without one stops on it.
// A routine is dropped without CASCADE in both styles; the exact style keeps
// CASCADE on a view-like drop that relies on it.
func (p *Planner) lossOutcome(kind string) string {
	const refused = "PostgreSQL refuses the drop while it does"
	switch {
	case kind == "routine":
		return refused
	case p.exact():
		return refused + ", and a drop with CASCADE would take it too, with nothing to create it again"
	default:
		return "the drop's CASCADE would drop it too, with nothing to create it again"
	}
}

// droppedReadable is an object the plan drops that a captured object may read
// or call: its name, its kind as a message names it, and the change a rebuild
// drops it for, or nil for an object the declaration removes.
type droppedReadable struct {
	name  string
	kind  string
	cause *rebuildCause
}

// droppedReadables is every view, materialized view and routine the plan drops
// while the database still holds the captured objects of the tables it keeps:
// the ones a view rebuild takes out, and the view-likes the declaration
// removes.
func droppedReadables(diff *difftypes.SchemaDiff, rebuild viewRebuild) []droppedReadable {
	var dropped []droppedReadable
	for _, object := range rebuild.recreate {
		cause := rebuild.causes[viewLikeKey(object)]
		dropped = append(dropped, droppedReadable{name: object.Name, kind: viewLikeKind(object), cause: &cause})
	}
	for _, object := range append(slices.Clone(rebuild.removeOnly), removedViewLikes(diff)...) {
		if !slices.ContainsFunc(dropped, func(seen droppedReadable) bool { return seen.name == object.Name }) {
			dropped = append(dropped, droppedReadable{name: object.Name, kind: viewLikeKind(object)})
		}
	}
	slices.SortStableFunc(dropped, func(a, b droppedReadable) int { return strings.Compare(a.name, b.name) })
	return dropped
}
