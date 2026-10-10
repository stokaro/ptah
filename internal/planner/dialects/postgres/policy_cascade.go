package postgres

import (
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
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
// The objects answer for themselves through [schemaext.RelationReader], so
// this planner names no owner. A diff built without a comparison carries no
// observation and is not checked.
func refusePoliciesLostToCascade(diff *difftypes.SchemaDiff) error {
	if diff.TablePreparation == nil {
		return nil
	}
	dropped := relationsDroppedWithCascade(diff)
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
				if reader.ReadsRelation(relation) {
					return fmt.Errorf("%w: the plan drops %s with CASCADE, which also drops %s, whose definition reads it, "+
						"and nothing recreates it; change it so it no longer reads %s first, then change %s",
						ptaherr.ErrUnsupportedFeature, relation, object.Ref, relation, relation)
				}
			}
		}
	}
	return nil
}

// relationsDroppedWithCascade names every view and materialized view the plan
// drops with CASCADE: a view it cannot replace in place, the view-like objects
// that read one, and the ones it changes or removes.
func relationsDroppedWithCascade(diff *difftypes.SchemaDiff) []string {
	var dropped []string
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Desired.Name != "" && !viewReplaceKeepsDependents(viewDiff, viewDiff.Desired.Body) {
			dropped = append(dropped, viewDiff.Desired.Name)
		}
	}
	semantics := diff.EffectiveIdentifierSemantics("")
	for _, lost := range viewLikesLostToCascade(diff.DeclaredViewLikes, slices.Clone(dropped), semantics) {
		dropped = append(dropped, lost.Name)
	}
	for _, viewDiff := range diff.MaterializedViewsModified {
		if viewDiff.Desired.Name != "" {
			dropped = append(dropped, viewDiff.Desired.Name)
		}
	}
	for _, view := range diff.ViewsRemoved {
		dropped = append(dropped, view.Name)
	}
	for _, view := range diff.MaterializedViewsRemoved {
		dropped = append(dropped, view.Name)
	}
	slices.Sort(dropped)
	return slices.Compact(dropped)
}
