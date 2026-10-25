package postgres

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// refusePoliciesLostToCascade refuses a plan whose DROP ... CASCADE of a view
// or materialized view would take a captured object with it, such as a
// row-level security policy whose expression reads the relation.
//
// The server records that dependency, so the cascade drops the object too. Its
// owner plans no change for an object that did not change, so nothing would
// put it back: the plan applies, and the table has lost a control the schema
// still declares (stokaro/ptah#4305). A lost permissive policy hides the rows
// it admitted; a lost restrictive one reveals the rows it hid. The plan is
// refused and names the object, and changing the object so it no longer reads
// the relation first is the way through.
//
// The refusal holds in both statement styles. A view rebuild drops its objects
// with CASCADE only in Ptah's own style; the exact style drops them without
// it, and the server refuses that drop while the policy reads the relation, so
// the plan could not apply either (stokaro/ptah#4965). The error says which of
// the two the plan would run into.
//
// The objects answer for themselves through [schemaext.RelationReader], so
// this planner names no owner. A diff built without a comparison carries no
// observation and is not checked.
func (p *Planner) refusePoliciesLostToCascade(diff *difftypes.SchemaDiff) error {
	if diff.TablePreparation == nil {
		return nil
	}
	dropped := droppedReadables(diff)
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
			if !ok {
				continue
			}
			for _, relation := range dropped {
				if reader.ReadsRelation(relation.name) {
					return p.lostReaderRefusal(relation, fmt.Sprint(object.Ref))
				}
			}
		}
	}
	return nil
}

// lostReaderRefusal is the refusal of a plan whose drop of relation would take
// the captured object reader with it, or stop on it.
func (p *Planner) lostReaderRefusal(relation droppedReadable, reader string) error {
	if relation.routine {
		return fmt.Errorf("%w: the plan drops and creates again the routine %s, which %s calls, and PostgreSQL refuses the drop "+
			"while it does; change it so it no longer calls %s first, then change what %s depends on",
			ptaherr.ErrUnsupportedFeature, relation.name, reader, relation.name, relation.name)
	}
	if p.exact() {
		return fmt.Errorf("%w: the plan drops %s, which %s reads, and PostgreSQL refuses the drop while it does; "+
			"nothing recreates it, so change it so it no longer reads %s first, then change %s",
			ptaherr.ErrUnsupportedFeature, relation.name, reader, relation.name, relation.name)
	}
	return fmt.Errorf("%w: the plan drops %s with CASCADE, which also drops %s, whose definition reads it, "+
		"and nothing recreates it; change it so it no longer reads %s first, then change %s",
		ptaherr.ErrUnsupportedFeature, relation.name, reader, relation.name, relation.name)
}

// droppedReadable is an object the plan drops that a captured object may read
// or call.
type droppedReadable struct {
	name    string
	routine bool
}

// droppedReadables names every view, materialized view and routine the plan
// drops while the database still holds the captured objects of the tables it
// keeps: the ones a view rebuild takes out, and the view-likes it removes. A
// routine is dropped without CASCADE in either style.
func droppedReadables(diff *difftypes.SchemaDiff) []droppedReadable {
	var dropped []droppedReadable
	for _, object := range planViewRebuild(diff).dropped() {
		dropped = append(dropped, droppedReadable{name: object.Name, routine: object.Routine})
	}
	for _, object := range removedViewLikes(diff) {
		dropped = append(dropped, droppedReadable{name: object.Name})
	}
	slices.SortFunc(dropped, func(a, b droppedReadable) int { return strings.Compare(a.name, b.name) })
	return slices.Compact(dropped)
}
