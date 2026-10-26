package postgres

import (
	"ptah.run/core/difftypes"
	"ptah.run/internal/deporder"
)

// readsRelationIn is the test a view rebuild finds the readers of a relation
// with: [deporder.ReferencesRelation] under the identifier semantics of the
// connection the diff was compared over, the ones the policy refusal asks
// with too.
//
// Without them an unqualified name may be in any schema, so a view that reads
// `item_skus` is rebuilt with reporting.item_skus, and a policy on it refuses
// the plan, while the view reads public.item_skus. A diff compared without a
// connection carries no semantics, and gets that answer.
func readsRelationIn(diff *difftypes.SchemaDiff) func(body, relation string) bool {
	semantics := diff.EffectiveIdentifierSemantics("")
	return func(body, relation string) bool {
		return deporder.ReferencesRelation(body, relation, semantics)
	}
}
