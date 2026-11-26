package mysql

import (
	"cmp"
	"slices"

	"ptah.run/core/difftypes"
)

// revokesByGrantor expands each permission the comparison removes to every
// catalog row holding it. The comparison groups by permission, while SQL
// Server's REVOKE removes the row of one grantor. Both the reach check and the
// emitted statements read this expansion: a plain row can hide another
// grantor's WITH GRANT OPTION and its dependent grants.
//
// An option-only revoke leaves a matching plain row alone: SQL Server can
// remove that row's base permission with REVOKE GRANT OPTION FOR. An offline
// reference with no matching catalog row keeps its supplied grantor. A
// reference without a captured grantor is a statement of its own, including
// the inverse of a new GRANT or DENY: it must not select grantors from the
// pre-change preservation context of a generated rollback.
func (p *Planner) revokesByGrantor(diff *difftypes.SchemaDiff, refs, current []difftypes.GrantRef, optionOnly bool) []difftypes.GrantRef {
	if !p.revokesAsGrantor() {
		return refs
	}
	identities := p.newDenies(diff)
	seen := make(map[difftypes.GrantRef]bool)
	var result []difftypes.GrantRef
	for _, ref := range refs {
		for _, held := range identities.rowsForRevoke(ref, current, optionOnly) {
			if !seen[held] {
				result = append(result, held)
				seen[held] = true
			}
		}
	}
	return result
}

// rowsForRevoke matches the permission and scope, including the default
// schema's spelling, then orders its grantors for deterministic SQL output.
func (d denies) rowsForRevoke(ref difftypes.GrantRef, current []difftypes.GrantRef, optionOnly bool) []difftypes.GrantRef {
	if ref.GrantedBy == "" {
		return []difftypes.GrantRef{ref}
	}
	want := d.key(ref)
	matched := false
	var rows []difftypes.GrantRef
	for _, held := range current {
		if d.key(held) != want || held.Arguments != ref.Arguments {
			continue
		}
		matched = true
		if !optionOnly || held.WithOption {
			rows = append(rows, held)
		}
	}
	if !matched {
		return []difftypes.GrantRef{ref}
	}
	slices.SortFunc(rows, func(a, b difftypes.GrantRef) int { return cmp.Compare(a.GrantedBy, b.GrantedBy) })
	return rows
}

// revokedDeniesByGrantor expands only actual DENY rows. SQL Server's column
// REVOKE rows share CurrentPartialRevokes, but are a different permission
// state and must not be taken as another grantor's DENY.
func (p *Planner) revokedDeniesByGrantor(diff *difftypes.SchemaDiff) []difftypes.GrantRef {
	denied := slices.DeleteFunc(slices.Clone(diff.CurrentPartialRevokes), func(ref difftypes.GrantRef) bool {
		return !ref.Denied
	})
	return p.revokesByGrantor(diff, diff.DeniesRemoved, denied, false)
}

// revokeGrantor returns the principal this target must name in a revoke.
func (p *Planner) revokeGrantor(grant difftypes.GrantRef) string {
	if p.revokesAsGrantor() {
		return grant.GrantedBy
	}
	return ""
}
