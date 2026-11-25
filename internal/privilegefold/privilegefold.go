// Package privilegefold composes GRANT, REVOKE and DENY declarations in the
// order a SQL schema reads them.
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
// in it.
//
// src's statements on whole objects are merged before its statements on
// columns. A statement on a whole object takes the ones of its privilege on
// the object's columns, so in a folded src every column statement beside one
// on its object came after it: `DENY SELECT ON t` then `GRANT SELECT (c) ON
// t`, or `GRANT SELECT ON t` then `DENY SELECT (c) ON t`. Merged by list
// rather than in that order, the statement on the object takes the column
// statement src keeps, and a schema directory folds a file differently from
// the file read alone.
//
// Column grants are folded one column at a time, each kept as a grant of one
// column. A revoke of a table privilege also takes that privilege off every
// column of the table, and a grant of a table privilege clears the revokes of
// it on any column: measured on PostgreSQL 18, REVOKE UPDATE ON t after GRANT
// UPDATE (a) ON t empties the column's ACL, and a table privilege covers every
// column.
//
// A revoke spelling ALL takes every privilege of its grantee on its object out
// of dst, whatever the privilege is called, and is recorded as ALL: what ALL
// names differs from one server to the next, and a list of it would leave
// whatever it misses held. A later grant of one privilege leaves the revoked
// ALL in place, so the two together say the grantee holds that privilege and
// nothing else on the object.
//
// A grant WITH GRANT OPTION gives back the option an earlier REVOKE GRANT
// OPTION FOR took ([schemamodel.Grant.GrantOptionRevoked]): the record leaves
// the privileges it names. Kept beside the later grant, it would ask a
// comparison to take an option the script ends up granting.
//
// A denied privilege ([schemamodel.Database.DeniedGrants]) takes the place of
// a grant and of a revoke of it, and a later grant or revoke takes the place
// of a denied one, as SQL Server keeps one state for a privilege of one
// grantee on one object. A privilege granted or denied on the whole table
// also takes the place of the grants and denies of it on the table's columns:
// measured on SQL Server 2025, `DENY SELECT ON t` after `GRANT SELECT (c) ON
// t`, and `GRANT SELECT ON t` after `DENY SELECT (c) ON t`, each leave one row
// for the table. A grant on a column after a DENY on its table keeps both, as
// the server does, and a DENY on a column under a DENY on its table adds
// nothing, as on the server.
func Merge(dst, src *schemamodel.Database) {
	for _, declared := range src.RevokedGrants {
		if len(declared.Privileges) == 0 {
			// Nothing to fold, and nothing to lose: carried as it came, so a
			// declaration that names no privilege still reaches the validation
			// that refuses it rather than vanishing here.
			dst.RevokedGrants = append(dst.RevokedGrants, declared)
		}
	}
	for _, denied := range src.DeniedGrants {
		if len(denied.Privileges) == 0 {
			dst.DeniedGrants = append(dst.DeniedGrants, denied)
		}
	}
	for _, granted := range src.Grants {
		if len(granted.Privileges) == 0 {
			dst.Grants = append(dst.Grants, granted)
		}
	}
	for _, scope := range []func(schemamodel.Grant) bool{onWholeObject, onColumns} {
		merge(dst, src.RevokedGrants, scope, mergeRevoke)
		merge(dst, src.Grants, scope, mergeGrant)
		merge(dst, src.DeniedGrants, scope, mergeDeny)
	}
}

// onWholeObject reports a statement on a whole object, one that names no
// column.
func onWholeObject(grant schemamodel.Grant) bool { return len(grant.Columns) == 0 }

// onColumns reports a statement on columns.
func onColumns(grant schemamodel.Grant) bool { return len(grant.Columns) > 0 }

// merge folds each of declared that names a privilege and is in scope into
// dst with fold, one column at a time.
func merge(dst *schemamodel.Database, declared []schemamodel.Grant, scope func(schemamodel.Grant) bool, fold func(*schemamodel.Database, schemamodel.Grant)) {
	for _, grant := range declared {
		if len(grant.Privileges) == 0 || !scope(grant) {
			continue
		}
		for _, column := range grant.ByColumn() {
			fold(dst, column)
		}
	}
}

func mergeDeny(dst *schemamodel.Database, denied schemamodel.Grant) {
	target := denied.TargetKey()
	for _, privilege := range denied.Privileges {
		dst.Grants = Without(dst.Grants, denied.Role, target, privilege)
		dst.RevokedGrants = slices.DeleteFunc(dst.RevokedGrants, func(revoked schemamodel.Grant) bool {
			return revoked.Role == denied.Role && revoked.TargetKey() == target && revokedPrivilege(revoked) == privilege
		})
		if tableWide(denied) {
			dst.Grants = withoutColumns(dst.Grants, denied.Role, denied.OnTable, privilege)
			dst.DeniedGrants = withoutColumns(dst.DeniedGrants, denied.Role, denied.OnTable, privilege)
		}
		wholeTable := schemamodel.Grant{OnTable: denied.OnTable}.TargetKey()
		underTable := len(denied.Columns) > 0 && isRevoked(dst.DeniedGrants, denied.Role, wholeTable, privilege)
		if !underTable && !isRevoked(dst.DeniedGrants, denied.Role, target, privilege) {
			entry := denied
			entry.Privileges = []string{privilege}
			dst.DeniedGrants = append(dst.DeniedGrants, entry)
		}
	}
}

func mergeRevoke(dst *schemamodel.Database, revoked schemamodel.Grant) {
	target := revoked.TargetKey()
	for _, privilege := range revoked.Privileges {
		dst.Grants = Without(dst.Grants, revoked.Role, target, privilege)
		dst.DeniedGrants = Without(dst.DeniedGrants, revoked.Role, target, privilege)
		if tableWide(revoked) {
			dst.Grants = withoutColumns(dst.Grants, revoked.Role, revoked.OnTable, privilege)
			dst.DeniedGrants = withoutColumns(dst.DeniedGrants, revoked.Role, revoked.OnTable, privilege)
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
	all := slices.Contains(grant.Privileges, allPrivileges)
	dst.RevokedGrants = slices.DeleteFunc(dst.RevokedGrants, func(revoked schemamodel.Grant) bool {
		sameTarget := revoked.TargetKey() == target ||
			(tableWide(grant) && revoked.OnTable == grant.OnTable && len(revoked.Columns) > 0)
		return revoked.Role == grant.Role && sameTarget &&
			(all || slices.Contains(grant.Privileges, revokedPrivilege(revoked)))
	})
	dst.DeniedGrants = slices.DeleteFunc(dst.DeniedGrants, func(denied schemamodel.Grant) bool {
		sameTarget := denied.TargetKey() == target ||
			(tableWide(grant) && denied.OnTable == grant.OnTable && len(denied.Columns) > 0)
		return denied.Role == grant.Role && sameTarget &&
			(all || slices.Contains(grant.Privileges, revokedPrivilege(denied)))
	})
	if grant.WithOption {
		dst.Grants = withoutRevokedOptions(dst.Grants, grant)
	}
	dst.Grants = append(dst.Grants, grant)
}

// withoutRevokedOptions takes the privileges grant names, or every privilege
// for ALL, out of each grant to the same grantee on the same object that
// records a revoked option, dropping one left with no privilege.
func withoutRevokedOptions(grants []schemamodel.Grant, grant schemamodel.Grant) []schemamodel.Grant {
	target := grant.TargetKey()
	all := slices.Contains(grant.Privileges, allPrivileges)
	kept := make([]schemamodel.Grant, 0, len(grants))
	for _, held := range grants {
		if held.GrantOptionRevoked && held.Role == grant.Role && held.TargetKey() == target {
			held.Privileges = slices.DeleteFunc(slices.Clone(held.Privileges), func(name string) bool {
				return all || slices.Contains(grant.Privileges, name)
			})
			if len(held.Privileges) == 0 {
				continue
			}
		}
		kept = append(kept, held)
	}
	return kept
}

// tableWide reports whether a grant is about a whole table rather than some
// of its columns.
func tableWide(grant schemamodel.Grant) bool {
	return grant.OnTable != "" && grant.OnSchema == "" && grant.OnSequence == "" &&
		grant.OnRoutine == "" && len(grant.Columns) == 0
}

// withoutColumns takes one privilege, or every privilege for ALL, out of every
// column grant of it to role on table, dropping a grant left with no
// privilege.
func withoutColumns(grants []schemamodel.Grant, role, table, privilege string) []schemamodel.Grant {
	kept := make([]schemamodel.Grant, 0, len(grants))
	for _, grant := range grants {
		if grant.Role == role && grant.OnTable == table && len(grant.Columns) > 0 {
			grant.Privileges = slices.DeleteFunc(slices.Clone(grant.Privileges), func(name string) bool {
				return names(privilege, name)
			})
			if len(grant.Privileges) == 0 {
				continue
			}
		}
		kept = append(kept, grant)
	}
	return kept
}

// TableGrantOf returns the first grant to role that gives privilege, or ALL,
// on the whole of a table onTable accepts, where privilege ALL asks about any
// grant there. onTable receives the grant's table as the file spells it, so the
// caller decides which spellings name one table. A revoke of that privilege on
// one column cannot take it away: the table privilege covers every column.
func TableGrantOf(grants []schemamodel.Grant, role, privilege string, onTable func(table string) bool) (schemamodel.Grant, bool) {
	index := slices.IndexFunc(grants, func(grant schemamodel.Grant) bool {
		return grant.Role == role && tableWide(grant) && onTable(grant.OnTable) &&
			slices.ContainsFunc(grant.Privileges, func(name string) bool { return names(privilege, name) || name == allPrivileges })
	})
	if index < 0 {
		return schemamodel.Grant{}, false
	}
	return grants[index], true
}

// Without takes one privilege, or every privilege for ALL, out of every grant
// of it to role on target, dropping a grant left with no privilege. It returns
// a new slice and leaves grants as it was.
func Without(grants []schemamodel.Grant, role, target, privilege string) []schemamodel.Grant {
	kept := make([]schemamodel.Grant, 0, len(grants))
	for _, grant := range grants {
		if grant.Role == role && grant.TargetKey() == target {
			grant.Privileges = slices.DeleteFunc(slices.Clone(grant.Privileges), func(name string) bool {
				return names(privilege, name)
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
		return grant.Role == role && grant.TargetKey() == target && slices.Contains(grant.Privileges, allPrivileges)
	})
}

// allPrivileges is the keyword a grant or a revoke spells for every privilege
// on its object.
const allPrivileges = "ALL"

// names reports whether a revoke of privilege takes the privilege called name:
// its own, or any for ALL.
func names(privilege, name string) bool {
	return privilege == allPrivileges || name == privilege
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
