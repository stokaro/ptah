package generator

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/routineargs"
)

// routineGrantSnapshotsForRollback carries the privileges the rollback must
// restore under the overload identity it will replace. After g(integer) has
// become g(bigint), the reverse planner looks up g(bigint), but must restore
// the privileges read from g(integer), before any declared grant changes.
// Other objects keep their original snapshots.
func routineGrantSnapshotsForRollback(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []difftypes.GrantRef {
	grants := slices.Clone(diff.CurrentGrants)
	for i, grant := range grants {
		switch strings.ToUpper(grant.ObjectType) {
		case "FUNCTION", "PROCEDURE", "ROUTINE":
		default:
			continue
		}
		for _, change := range diff.FunctionsModified {
			_, parametersChanged := change.Changes["parameters"]
			if !parametersChanged || change.CurrentSignature == nil ||
				semantics.QualifiedTableIdentityKey(grant.ObjectName) != semantics.QualifiedTableIdentityKey(cmp.Or(change.CurrentName, change.FunctionName)) ||
				routineargs.InputTypes(grant.Arguments) != routineargs.InputTypes(*change.CurrentSignature) {
				continue
			}
			grants[i].ObjectName = change.FunctionName
			grants[i].Arguments = routineargs.InputTypes(change.Desired.Parameters)
			break
		}
	}
	return grants
}
