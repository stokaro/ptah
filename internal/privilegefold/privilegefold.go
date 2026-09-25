// Package privilegefold composes GRANT and REVOKE declarations in the order a
// SQL schema reads them.
//
// A SQL schema file is a script, and so is a schema directory read file by
// file. The privileges it declares are the ones the script leaves behind: a
// GRANT adds, a REVOKE takes away, and the later statement about one privilege
// of one grantee on one object wins. The statement reader in internal/sqlschema
// folds one file this way and internal/schemafile folds files onto each other;
// both go through [Merge], so the two cannot come to disagree about what "the
// same grant" means.
//
// A REVOKE is kept even when nothing before it granted the privilege. Measured
// on PostgreSQL 18, a privilege can be held without a GRANT the schema wrote: a
// function is executable by PUBLIC from the moment it is created, and ALTER
// DEFAULT PRIVILEGES hands a new table's privileges to a role. So a revoked
// privilege is recorded in [schemamodel.Database.RevokedGrants] as asserted
// absent, and a comparison plans the REVOKE a database still needs.
package privilegefold

import (
	"slices"

	"ptah.run/core/schemamodel"
)

// Merge adds src's grants and revoked grants to dst as though src's statements
// ran after dst's.
//
// A privilege src revokes leaves every grant of it in dst and joins dst's
// revoked set once; one src grants leaves dst's revoked set. A GRANT spelling
// ALL takes every revoked privilege of its grantee on its object back out. src
// is expected to be folded already, so no privilege is both granted and revoked
// in it, and the order the two lists are applied in cannot change the result.
//
// Column grants are folded one column at a time, each kept as a grant of one
// column. A revoke of a table privilege also takes that privilege off every
// column of the table, and a grant of a table privilege clears the revokes of
// it on any column: measured on PostgreSQL 18, REVOKE UPDATE ON t after GRANT
// UPDATE (a) ON t empties the column's ACL, and a table privilege covers every
// column.
func Merge(dst, src *schemamodel.Database) {
	for _, declared := range src.RevokedGrants {
		if len(declared.Privileges) == 0 {
			// Nothing to fold, and nothing to lose: carried as it came, so a
			// declaration that names no privilege still reaches the validation
			// that refuses it rather than vanishing here.
			dst.RevokedGrants = append(dst.RevokedGrants, declared)
			continue
		}
		for _, revoked := range declared.ByColumn() {
			mergeRevoke(dst, revoked)
		}
	}
	for _, declared := range src.Grants {
		for _, grant := range declared.ByColumn() {
			mergeGrant(dst, grant)
		}
	}
}

func mergeRevoke(dst *schemamodel.Database, revoked schemamodel.Grant) {
	target := revoked.TargetKey()
	for _, privilege := range revoked.Privileges {
		dst.Grants = Without(dst.Grants, revoked.Role, target, privilege)
		if tableWide(revoked) {
			dst.Grants = withoutColumns(dst.Grants, revoked.Role, revoked.OnTable, privilege)
		}
		if !isRevoked(dst.RevokedGrants, revoked.Role, target, privilege) {
			entry := revoked
			entry.Privileges = []string{privilege}
			dst.RevokedGrants = append(dst.RevokedGrants, entry)
		}
	}
}

func mergeGrant(dst *schemamodel.Database, grant schemamodel.Grant) {
	target := grant.TargetKey()
	all := slices.Contains(grant.Privileges, "ALL")
	dst.RevokedGrants = slices.DeleteFunc(dst.RevokedGrants, func(revoked schemamodel.Grant) bool {
		sameTarget := revoked.TargetKey() == target ||
			(tableWide(grant) && revoked.OnTable == grant.OnTable && len(revoked.Columns) > 0)
		return revoked.Role == grant.Role && sameTarget &&
			(all || slices.Contains(grant.Privileges, revokedPrivilege(revoked)))
	})
	dst.Grants = append(dst.Grants, grant)
}

// tableWide reports whether a grant is about a whole table rather than some
// of its columns.
func tableWide(grant schemamodel.Grant) bool {
	return grant.OnTable != "" && grant.OnSchema == "" && grant.OnSequence == "" &&
		grant.OnRoutine == "" && len(grant.Columns) == 0
}

// withoutColumns takes one privilege out of every column grant of it to role
// on table, dropping a grant left with no privilege.
func withoutColumns(grants []schemamodel.Grant, role, table, privilege string) []schemamodel.Grant {
	kept := make([]schemamodel.Grant, 0, len(grants))
	for _, grant := range grants {
		if grant.Role == role && grant.OnTable == table && len(grant.Columns) > 0 {
			grant.Privileges = slices.DeleteFunc(slices.Clone(grant.Privileges), func(name string) bool {
				return name == privilege
			})
			if len(grant.Privileges) == 0 {
				continue
			}
		}
		kept = append(kept, grant)
	}
	return kept
}

// HeldOnTable reports whether a grant to role gives privilege, or ALL, on the
// whole of table. A revoke of that privilege on one column cannot take it
// away: the table privilege covers every column.
func HeldOnTable(grants []schemamodel.Grant, role, table, privilege string) bool {
	return slices.ContainsFunc(grants, func(grant schemamodel.Grant) bool {
		return grant.Role == role && tableWide(grant) && grant.OnTable == table &&
			(slices.Contains(grant.Privileges, privilege) || slices.Contains(grant.Privileges, "ALL"))
	})
}

// Without takes one privilege out of every grant of it to role on target,
// dropping a grant left with no privilege. It returns a new slice and leaves
// grants as it was.
func Without(grants []schemamodel.Grant, role, target, privilege string) []schemamodel.Grant {
	kept := make([]schemamodel.Grant, 0, len(grants))
	for _, grant := range grants {
		if grant.Role == role && grant.TargetKey() == target {
			grant.Privileges = slices.DeleteFunc(slices.Clone(grant.Privileges), func(name string) bool {
				return name == privilege
			})
			if len(grant.Privileges) == 0 {
				continue
			}
		}
		kept = append(kept, grant)
	}
	return kept
}

// HeldAsAll reports whether a grant to role on target spells ALL, which a
// single privilege cannot be taken out of without knowing the server's list.
func HeldAsAll(grants []schemamodel.Grant, role, target string) bool {
	return slices.ContainsFunc(grants, func(grant schemamodel.Grant) bool {
		return grant.Role == role && grant.TargetKey() == target && slices.Contains(grant.Privileges, "ALL")
	})
}

func isRevoked(revoked []schemamodel.Grant, role, target, privilege string) bool {
	return slices.ContainsFunc(revoked, func(grant schemamodel.Grant) bool {
		return grant.Role == role && grant.TargetKey() == target && revokedPrivilege(grant) == privilege
	})
}

// revokedPrivilege is the one privilege a folded revoked grant carries, and
// empty for a declaration that named none, which [Merge] carries unfolded.
func revokedPrivilege(revoked schemamodel.Grant) string {
	if len(revoked.Privileges) == 0 {
		return ""
	}
	return revoked.Privileges[0]
}

// MergeDefaultPrivileges returns dst with src's default privileges applied as
// though src's statements ran after dst's, the default-privilege counterpart
// of [Merge].
//
// Declarations are matched by their identity -- grantor, schema, object class
// and grantee -- which is what schemamodel.Deduplicate keys them on. A
// privilege src revokes leaves the matching declaration's Privileges and joins
// its Revoked; a revoked ALL leaves none of its Privileges. A privilege src
// grants leaves the matching declaration's Revoked, and a granted ALL clears
// it. A declaration with no match in dst is appended. Neither input is
// modified.
func MergeDefaultPrivileges(dst, src []schemamodel.DefaultPrivilege) []schemamodel.DefaultPrivilege {
	merged := slices.Clone(dst)
	for _, declaration := range src {
		declaration.Canonicalize()
		index := slices.IndexFunc(merged, func(existing schemamodel.DefaultPrivilege) bool {
			return sameDefaultPrivilege(existing, declaration)
		})
		if index < 0 {
			merged = append(merged, declaration)
			continue
		}
		existing := merged[index]
		existing.Privileges = slices.DeleteFunc(slices.Clone(existing.Privileges), func(grant schemamodel.PrivilegeGrant) bool {
			return slices.Contains(declaration.Revoked, grant.Privilege) || slices.Contains(declaration.Revoked, "ALL")
		})
		existing.Revoked = append(slices.Clone(existing.Revoked), declaration.Revoked...)
		granted := make([]string, 0, len(declaration.Privileges))
		for _, grant := range declaration.Privileges {
			granted = append(granted, grant.Privilege)
		}
		existing.Revoked = slices.DeleteFunc(existing.Revoked, func(privilege string) bool {
			return slices.Contains(granted, privilege) || slices.Contains(granted, "ALL")
		})
		existing.Privileges = append(existing.Privileges, declaration.Privileges...)
		existing.Canonicalize()
		merged[index] = existing
	}
	return merged
}

// sameDefaultPrivilege reports whether two declarations name one default
// privilege object.
func sameDefaultPrivilege(a, b schemamodel.DefaultPrivilege) bool {
	a.Canonicalize()
	b.Canonicalize()
	return a.Grantor == b.Grantor && a.Schema == b.Schema && a.ObjectType == b.ObjectType && a.Grantee == b.Grantee
}
