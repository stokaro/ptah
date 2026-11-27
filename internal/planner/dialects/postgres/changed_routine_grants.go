package postgres

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

// changedRoutineGrants gives a replacement the privileges its old overload
// held. A declaration can change those privileges in the usual grant phase;
// the snapshot also covers roles the declaration does not manage.
func changedRoutineGrants(change difftypes.FunctionDiff, diff *difftypes.SchemaDiff, semantics identifier.Semantics) []ast.Node {
	if change.CurrentSignature == nil || !replacementIsRefused(change) {
		return nil
	}
	held := modifiedRoutineBefore(change)
	var nodes []ast.Node
	public := false
	for _, grant := range diff.CurrentGrants {
		if !held.grantedBy(grant, semantics) || grant.Privilege == "" {
			continue
		}
		restored := grant
		restored.ObjectName = change.Desired.Name
		restored.ObjectType = routineObjectType(change.Desired.Kind)
		restored.Arguments = routineargs.InputTypes(change.Desired.Parameters)
		removed := routineGrantIn(diff.GrantsRemoved, grant, semantics) || routineGrantIn(diff.GrantsRemoved, restored, semantics)
		if grant.Role == "PUBLIC" && grant.Privilege == "EXECUTE" && !removed {
			public = true
		}
		if removed || grantPlanned(diff, restored, semantics) {
			continue
		}
		if routineGrantIn(diff.GrantOptionsRevoked, grant, semantics) || routineGrantIn(diff.GrantOptionsRevoked, restored, semantics) {
			restored.WithOption = false
		}
		nodes = append(nodes, grantNode(restored))
	}
	// A comparison leaves the snapshot nil when grants were not read. A
	// non-nil, empty snapshot can be the owner alone after PUBLIC's EXECUTE
	// was revoked; the new routine must not reopen that access.
	if diff.CurrentGrants != nil && !public {
		nodes = append(nodes, ast.NewRevokePrivilege("PUBLIC", routineObjectType(change.Desired.Kind), change.Desired.Name, []string{"EXECUTE"}).
			SetArguments(routineargs.InputTypes(change.Desired.Parameters)))
	}
	return nodes
}

// modifiedRoutineBefore is the overload a modification replaces, under the
// read's name when the declaration leaves its schema out.
func modifiedRoutineBefore(change difftypes.FunctionDiff) heldRoutine {
	return heldRoutine{RoutineChange: difftypes.RoutineChange{
		Function:  schemamodel.Function{Name: cmp.Or(change.CurrentName, change.FunctionName), Kind: change.Desired.Kind},
		Signature: change.CurrentSignature,
	}}
}

// routineGrantIn matches one privilege across the declaration's and read's
// spellings of its overload. The grant option changes the privilege's state,
// not its identity.
func routineGrantIn(grants []difftypes.GrantRef, grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(grants, func(planned difftypes.GrantRef) bool {
		return routineObjectTypes[strings.ToUpper(planned.ObjectType)] && planned.Role == grant.Role && planned.Privilege == grant.Privilege &&
			sameObject(planned.ObjectName, grant.ObjectName, semantics) &&
			routineargs.InputTypes(planned.Arguments) == routineargs.InputTypes(grant.Arguments)
	})
}

// modifiedRoutineDropsGrant reports that a privilege names the old identity
// of a retyped routine. DROP already took it, and a late GRANT or REVOKE on
// that identity would fail because only the new overload exists now. A
// parameter rename or return-type change keeps the input-type identity, so
// the ordinary grant phase can still address the replacement.
func modifiedRoutineDropsGrant(diff *difftypes.SchemaDiff, grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	for _, change := range diff.FunctionsModified {
		if change.CurrentSignature != nil && replacementIsRefused(change) &&
			routineargs.InputTypes(*change.CurrentSignature) != routineargs.InputTypes(change.Desired.Parameters) &&
			modifiedRoutineBefore(change).grantedBy(grant, semantics) {
			return true
		}
	}
	return false
}

// rebuiltRoutineChange finds a modified overload that a view rebuild drops
// and recreates. It needs the same privilege restoration as a replacement
// placed outside that rebuild, including when its signature changes.
func rebuiltRoutineChange(held heldRoutine, diff *difftypes.SchemaDiff, semantics identifier.Semantics) (difftypes.FunctionDiff, bool) {
	for _, change := range diff.FunctionsModified {
		if change.CurrentSignature != nil && replacementIsRefused(change) && held.grantedBy(difftypes.GrantRef{
			ObjectType: routineObjectType(change.Desired.Kind), ObjectName: cmp.Or(change.CurrentName, change.FunctionName),
			Arguments: *change.CurrentSignature,
		}, semantics) {
			return change, true
		}
	}
	return difftypes.FunctionDiff{}, false
}
