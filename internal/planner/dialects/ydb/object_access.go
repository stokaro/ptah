package ydb

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/internal/ydbacl"
	"ptah.run/migration/schemadiff/difftypes"
)

// objectLifecycle is what a plan does to each scheme object an owner manages
// and a grant can name, such as a topic: the objects it drops, and the ones
// it drops and creates again. Either takes the object's permission entries
// with it. An object the plan creates holds no entry of its own, and one it
// changes in place keeps its entries: measured on 26.2.1.14, ALTER TOPIC,
// ALTER SECRET, a change of a coordination node, CREATE OR REPLACE of an
// external object, ALTER ASYNC REPLICATION, ALTER TRANSFER, ALTER STREAMING
// QUERY and ALTER RESOURCE POOL each keep them. No owner of an async
// replication, a transfer, a streaming query or a resource pool drops one to
// create it again.
//
// changedLast are the objects whose owner places its statements on them after
// every statement of the host, which a GRANT on one has to follow: a streaming
// query the plan creates or changes runs against the schema the rest of the
// plan leaves.
type objectLifecycle struct {
	dropped     map[objectidentity.Key]bool
	recreated   map[objectidentity.Key]bool
	changedLast map[objectidentity.Key]bool
}

// ownedObjectLifecycle reads the owners' changes of diff, on a target with
// caps, for the objects they drop or drop and create again.
func ownedObjectLifecycle(diff *difftypes.SchemaDiff, caps capability.Capabilities) objectLifecycle {
	lifecycle := objectLifecycle{dropped: make(map[objectidentity.Key]bool), recreated: make(map[objectidentity.Key]bool),
		changedLast: make(map[objectidentity.Key]bool)}
	displaced := ydbdiff.DisplacedSources(diff.FeatureChanges, caps)
	for _, record := range diff.FeatureChanges {
		key := record.Subject.Key()
		switch change := record.Value.(type) {
		case *ydbdiff.Topic:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		case *ydbdiff.CoordinationNode:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		case *ydbdiff.Secret:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		case *ydbdiff.ExternalDataSource:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
			lifecycle.recreated[key] = change.Recreates(caps)
		case *ydbdiff.ExternalTable:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
			lifecycle.recreated[key] = change.Recreates(caps, diff.CurrentDatabasePath, displaced)
		case *ydbdiff.AsyncReplication:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		case *ydbdiff.Transfer:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		case *ydbdiff.StreamingQuery:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
			lifecycle.changedLast[key] = change.After != nil
		case *ydbdiff.ResourcePool:
			lifecycle.dropped[key] = change.Before != nil && change.After == nil
		}
	}
	return lifecycle
}

// objectOf is the owner's identity of the scheme object grant names, and
// false for a grant on the database, a directory or a table.
func objectOf(grant difftypes.GrantRef, root string) (objectidentity.ID, bool) {
	return ydbacl.ObjectRef(grant.ObjectType, grant.ObjectName, root)
}

// revokedWithObject reports whether the plan takes grant's entry away by
// dropping the object it is on, with or without creating it again, so no
// REVOKE is written for it: a REVOKE after the drop would name an object that
// is gone.
func (l objectLifecycle) revokedWithObject(grant difftypes.GrantRef, root string) bool {
	ref, ok := objectOf(grant, root)
	return ok && (l.dropped[ref.Key()] || l.recreated[ref.Key()])
}

// grantsLast reports whether a GRANT of grant's entry goes after every
// statement the plan schedules, because the owner of its object places the
// object's creation or change there; see [objectLifecycle].
func (l objectLifecycle) grantsLast(grant difftypes.GrantRef, root string) bool {
	ref, ok := objectOf(grant, root)
	return ok && l.changedLast[ref.Key()]
}

// grantedOnDroppedObject is the first grant of grants on an object the plan
// drops, which no GRANT can reach.
func (l objectLifecycle) grantedOnDroppedObject(grants []difftypes.GrantRef, root string) (difftypes.GrantRef, bool) {
	for _, grant := range grants {
		if ref, ok := objectOf(grant, root); ok && l.dropped[ref.Key()] {
			return grant, true
		}
	}
	return difftypes.GrantRef{}, false
}

// regrants are the permission entries each object the plan drops and creates
// again held, which the plan grants again on the new object. An entry the plan
// removes or grants anyway is left out.
func (l objectLifecycle) regrants(diff *difftypes.SchemaDiff, root string) []difftypes.GrantRef {
	if len(l.recreated) == 0 {
		return nil
	}
	planned := make(map[objectEntry]bool, len(diff.GrantsRemoved)+len(diff.GrantsAdded))
	for _, grant := range slices.Concat(diff.GrantsRemoved, diff.GrantsAdded) {
		if entry, ok := entryOf(grant, root); ok {
			planned[entry] = true
		}
	}
	var again []difftypes.GrantRef
	for _, grant := range diff.CurrentGrants {
		entry, ok := entryOf(grant, root)
		if !ok || !l.recreated[entry.object] || planned[entry] {
			continue
		}
		again = append(again, grant)
	}
	return again
}

// objectEntry is what makes two references to a scheme object's permission one
// entry: the subject, the permission, matched without regard to case, and the
// owner's identity of the object.
type objectEntry struct {
	role, permission string
	object           objectidentity.Key
}

// entryOf is the entry grant names, and false for a grant on the database, a
// directory or a table.
func entryOf(grant difftypes.GrantRef, root string) (objectEntry, bool) {
	ref, ok := objectOf(grant, root)
	if !ok {
		return objectEntry{}, false
	}
	return objectEntry{role: grant.Role, permission: strings.ToLower(grant.Privilege), object: ref.Key()}, true
}

// permissionEdges orders each GRANT and REVOKE on a scheme object an owner
// manages against the owner's statements on that object: a GRANT after every
// statement that creates or changes the object, so it reaches the object the
// plan leaves, and a REVOKE before every statement that drops it.
// [plangraph.LifecycleDependencies] orders any read before a drop, which is
// right for a REVOKE and wrong for a GRANT on an object the plan drops and
// creates again; these edges come first, so it leaves those pairs alone. A
// REVOKE and an in-place change are left unordered, since the change keeps the
// object's entries.
func permissionEdges(common plangraph.Contribution[[]ast.Node], contributions []plangraph.Contribution[[]ast.Node]) []plangraph.Dependency {
	type write struct {
		step   plangraph.StepID
		action plangraph.Action
	}
	writes := make(map[objectidentity.Key][]write)
	for _, contribution := range contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if effect.Action != plangraph.Read {
					writes[effect.Subject.Key()] = append(writes[effect.Subject.Key()], write{step.ID, effect.Action})
				}
			}
		}
	}
	var edges []plangraph.Dependency
	for _, step := range common.Steps {
		grant, revoke := permissionStatement(step.Payload)
		for _, effect := range step.Effects {
			for _, write := range writes[effect.Subject.Key()] {
				switch {
				case grant && (write.action == plangraph.Create || write.action == plangraph.Alter):
					edges = append(edges, plangraph.Dependency{Before: write.step, After: step.ID})
				case revoke && write.action == plangraph.Drop:
					edges = append(edges, plangraph.Dependency{Before: step.ID, After: write.step})
				}
			}
		}
	}
	return edges
}

// permissionStatement reports whether payload is a GRANT or a REVOKE.
func permissionStatement(payload []ast.Node) (grant, revoke bool) {
	for _, node := range payload {
		switch node.(type) {
		case *ast.GrantPrivilegeNode:
			grant = true
		case *ast.RevokePrivilegeNode:
			revoke = true
		}
	}
	return grant, revoke
}
