package postgres

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// refusePoliciesLostToCascade refuses a plan whose DROP ... CASCADE of a view
// or materialized view would take a row-level security policy with it.
//
// A policy whose expression reads a relation depends on it, so the cascade
// drops the policy too. The row-security owner holds the policy and plans no
// change for one that did not change, so nothing would put it back: the plan
// applies, and the table has lost a control the schema still declares
// (stokaro/ptah#4305). A lost permissive policy hides the rows it admitted; a
// lost restrictive one reveals the rows it hid. The plan is refused and names
// the policy, and changing the relation and the policy in separate steps, the
// policy first, is the way through.
//
// A policy reads a relation when its stored expression names it, the same
// syntactic test the view recreation makes. A false match refuses a plan that
// was safe, which is the side to err on. A diff built without a comparison
// carries no observation and is not checked.
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
			policy, ok := object.Value.(*pgpolicy.ObservedPolicy)
			if !ok {
				continue
			}
			for _, relation := range dropped {
				if readsRelation(policy, relation) {
					return fmt.Errorf("%w: the plan drops %s with CASCADE, which also drops policy %q on %s, "+
						"whose expression reads it, and nothing recreates the policy; change the policy so it no longer "+
						"reads %s first, then change %s", ptaherr.ErrUnsupportedFeature, relation, object.Ref.Name.Source,
						pgpolicy.Table(object.Ref), relation, relation)
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

// readsRelation reports whether either clause of policy names relation, by
// its full name or, for a qualified one, by its bare name.
func readsRelation(policy *pgpolicy.ObservedPolicy, relation string) bool {
	names := []string{relation}
	if ref, ok := tableref.Parse(relation); ok && ref.Qualified {
		names = append(names, ref.Name)
	}
	for _, clause := range []*string{policy.Using, policy.WithCheck} {
		if clause == nil {
			continue
		}
		for _, name := range names {
			if deporder.ReferencesIdentifier(*clause, strings.Trim(name, `"`)) {
				return true
			}
		}
	}
	return false
}
