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
func Merge(dst, src *schemamodel.Database) {
	for _, revoked := range src.RevokedGrants {
		if len(revoked.Privileges) == 0 {
			// Nothing to fold, and nothing to lose: carried as it came, so a
			// declaration that names no privilege still reaches the validation
			// that refuses it rather than vanishing here.
			dst.RevokedGrants = append(dst.RevokedGrants, revoked)
			continue
		}
		target := revoked.TargetKey()
		for _, privilege := range revoked.Privileges {
			dst.Grants = Without(dst.Grants, revoked.Role, target, privilege)
			if !isRevoked(dst.RevokedGrants, revoked.Role, target, privilege) {
				entry := revoked
				entry.Privileges = []string{privilege}
				dst.RevokedGrants = append(dst.RevokedGrants, entry)
			}
		}
	}
	for _, grant := range src.Grants {
		target := grant.TargetKey()
		all := slices.Contains(grant.Privileges, "ALL")
		dst.RevokedGrants = slices.DeleteFunc(dst.RevokedGrants, func(revoked schemamodel.Grant) bool {
			return revoked.Role == grant.Role && revoked.TargetKey() == target &&
				(all || slices.Contains(grant.Privileges, revokedPrivilege(revoked)))
		})
		dst.Grants = append(dst.Grants, grant)
	}
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
