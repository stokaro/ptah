package mysql

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
)

// refuseAWiderGrantOptionRevoke refuses a plan whose revoke of a grant option
// takes more than the diff asks for, where the engine decides the reach of the
// statement rather than the plan.
//
// On SQL Server the revoke carries CASCADE: without it the server answers Msg
// 4611 whether or not the grantee granted the privilege onward, and with it
// the grants the grantee made go too. See [Planner.refuseCascadedGrants].
//
// On MySQL and MariaDB the option is the grantee's at the whole object, so
// `REVOKE GRANT OPTION ON t` takes it from every privilege the grantee holds
// on t. See [refuseObjectWideGrantOptionRevokes].
func (p *Planner) refuseAWiderGrantOptionRevoke(diff *difftypes.SchemaDiff) error {
	dropped := p.droppedObjects(diff)
	switch {
	case p.revokesGrantOptionFirst():
		return refuseCascadedGrants(p.grantOptionRevokes(diff, dropped), diff, dropped)
	case p.grantOptionCoversTheObject():
		return refuseObjectWideGrantOptionRevokes(diff, dropped, p.targetDialect())
	default:
		return nil
	}
}

// grantOptionRevokes is every grant whose option the plan takes on SQL
// Server: the options the diff revokes, and the option of each grantable
// privilege it removes, which [Planner.revokeGrants] takes first.
func (p *Planner) grantOptionRevokes(diff *difftypes.SchemaDiff, dropped droppedObjects) []difftypes.GrantRef {
	revokes := slices.DeleteFunc(slices.Clone(diff.GrantOptionsRevoked), dropped.holds)
	for _, grant := range diff.GrantsRemoved {
		if grant.WithOption && !dropped.holds(grant) {
			revokes = append(revokes, grant)
		}
	}
	return revokes
}

// refuseCascadedGrants refuses a revoke of a grant option on SQL Server where
// the grantee granted the privilege onward and the plan does not revoke what
// it granted.
//
// Measured on SQL Server 2025: q2 holds SELECT on t48 WITH GRANT OPTION and
// grants SELECT to q2v, and `REVOKE GRANT OPTION FOR SELECT ON t48 FROM q2
// CASCADE` takes q2v's SELECT. The server keeps a row for each grantor, so a
// grant q2v holds from another grantor stays. A plan that runs the revoke takes
// a privilege from a principal the schema never names, and the next plan reads
// as synced. Without onward grants CASCADE takes nothing more, and the plan
// stands.
func refuseCascadedGrants(revokes []difftypes.GrantRef, diff *difftypes.SchemaDiff, dropped droppedObjects) error {
	for _, revoke := range revokes {
		var dependents []string
		for _, held := range diff.CurrentGrants {
			if !grantedOnwardFrom(held, revoke) || dropped.holds(held) || removes(diff.GrantsRemoved, held) {
				continue
			}
			dependents = append(dependents, granteeLabel(held))
		}
		if len(dependents) == 0 {
			continue
		}
		slices.Sort(dependents)
		return fmt.Errorf(
			"%w: sqlserver: cannot revoke the grant option for %s on %s from %q: SQL Server revokes it with CASCADE, "+
				"which also takes the %s that %q granted to %s, and the schema does not revoke those grants; "+
				"revoke them first, or declare their REVOKE in the schema",
			ptaherr.ErrUnsupportedFeature, revoke.Privilege, revoke.ObjectName, revoke.Role,
			revoke.Privilege, revoke.Role, strings.Join(slices.Compact(dependents), ", "),
		)
	}
	return nil
}

// grantedOnwardFrom reports whether held is a grant of revoke's privilege on
// its object that revoke's grantee made to another principal: on the whole
// object, or on a column the revoke reaches.
func grantedOnwardFrom(held, revoke difftypes.GrantRef) bool {
	return held.GrantedBy == revoke.Role && held.Role != revoke.Role &&
		held.Privilege == revoke.Privilege && held.ObjectType == revoke.ObjectType &&
		held.ObjectName == revoke.ObjectName && held.Arguments == revoke.Arguments &&
		(revoke.Column == "" || held.Column == revoke.Column)
}

// removes reports whether the plan revokes held itself.
func removes(removed []difftypes.GrantRef, held difftypes.GrantRef) bool {
	return slices.ContainsFunc(removed, func(grant difftypes.GrantRef) bool {
		return grant.Role == held.Role && grant.Privilege == held.Privilege && grant.ObjectType == held.ObjectType &&
			grant.ObjectName == held.ObjectName && grant.Arguments == held.Arguments && grant.Column == held.Column
	})
}

// granteeLabel is how a refusal names the principal a grant is to: with its
// column, for a grant on one.
func granteeLabel(grant difftypes.GrantRef) string {
	if grant.Column == "" {
		return fmt.Sprintf("%q", grant.Role)
	}
	return fmt.Sprintf("%q (%s)", grant.Role, grant.Column)
}

// refuseObjectWideGrantOptionRevokes refuses a revoke of a grant option on
// MySQL or MariaDB where the grantee holds another privilege on the object
// with the option, and the diff keeps that one grantable.
//
// Measured on MySQL 8.4.13 and MariaDB 11.8.9: a role holding SELECT and
// INSERT on t1 WITH GRANT OPTION, after `REVOKE GRANT OPTION ON t1 FROM r`,
// holds neither grantable. A schema that takes the option for SELECT alone
// asks to keep INSERT grantable, which the server cannot do, and the next plan
// reads as synced. The declared form of the same conflict is refused before
// anything is compared ([schemamodel.ValidateGrantOptionConsistency]); this is
// the form a database holds.
func refuseObjectWideGrantOptionRevokes(diff *difftypes.SchemaDiff, dropped droppedObjects, dialect string) error {
	revoked := make(map[grantObject][]string)
	var order []grantObject
	for _, grant := range slices.Concat(diff.GrantOptionsRevoked, diff.GrantsRemoved) {
		object := grantObjectOf(grant)
		if dropped.holds(grant) {
			continue
		}
		if _, seen := revoked[object]; !seen {
			order = append(order, object)
		}
		revoked[object] = append(revoked[object], privilegeOnColumn(grant))
	}
	for _, object := range order {
		if !takesAnOption(diff.GrantOptionsRevoked, object) {
			continue
		}
		var kept []string
		for _, held := range diff.CurrentGrants {
			if grantObjectOf(held) != object || !held.WithOption || held.Privilege == usageGrantOptionGhost {
				continue
			}
			if name := privilegeOnColumn(held); !slices.Contains(revoked[object], name) {
				kept = append(kept, name)
			}
		}
		if len(kept) == 0 {
			continue
		}
		slices.Sort(kept)
		return fmt.Errorf(
			"%w: %s: cannot revoke the grant option for %s on %s from %q: the server keeps the grant option at the whole "+
				"object, not at one privilege, so the revoke also takes it from %s, which the schema keeps grantable; "+
				"revoke the grant option for every privilege %q holds on %s, or for none of them",
			ptaherr.ErrUnsupportedFeature, platform.NormalizeDialect(dialect), strings.Join(optionsTaken(diff.GrantOptionsRevoked, object), ", "),
			object.objectName, object.role, strings.Join(slices.Compact(kept), ", "), object.role, object.objectName,
		)
	}
	return nil
}

// takesAnOption reports whether the diff revokes a grant option on object.
func takesAnOption(revokes []difftypes.GrantRef, object grantObject) bool {
	return slices.ContainsFunc(revokes, func(grant difftypes.GrantRef) bool { return grantObjectOf(grant) == object })
}

// optionsTaken names the privileges whose option the diff revokes on object.
func optionsTaken(revokes []difftypes.GrantRef, object grantObject) []string {
	var names []string
	for _, grant := range revokes {
		if grantObjectOf(grant) == object {
			names = append(names, privilegeOnColumn(grant))
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// privilegeOnColumn is how a refusal names a privilege: with its column, for
// one on a column.
func privilegeOnColumn(grant difftypes.GrantRef) string {
	if grant.Column == "" {
		return grant.Privilege
	}
	return grant.Privilege + " (" + grant.Column + ")"
}
