package clickhouserbac

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// ValidateLive refuses live ClickHouse state Ptah cannot compare against a
// declaration, and returns nil for every other dialect.
//
// The first shape is the one thing about ClickHouse grants that does not
// transfer from PostgreSQL: a PARTIAL REVOKE. Measured on 26.7.3.19,
//
//	GRANT SELECT ON db.* TO r ; REVOKE SELECT ON db.t FROM r
//
// leaves TWO rows in system.grants — the grant with is_partial_revoke = 0 and a
// second row with is_partial_revoke = 1 — and the role's effective privileges
// are the first minus the second. SHOW GRANTS prints both lines.
//
// Ptah's grant model has no shape for an exception. A declaration says "this
// role holds SELECT on db.*", and it cannot say "…except on db.t". So the
// comparator, seeing the grant row it recognizes and nothing it can attach the
// exception to, would compare equal and plan nothing — leaving Ptah silently
// tolerating a privilege reduction nobody declared, on a role it manages. That
// is worse than refusing, because it reads as convergence.
//
// The refusal is scoped to MANAGED roles, the same boundary
// migration/schemadiff/internal/compare draws for revocation: a partial revoke
// on a role no declaration names is somebody else's arrangement and is left
// alone.
//
// The second shape is a declared revoke on a table the role keeps through a
// grant on its whole database, for any role; see [revokesUnderDatabaseGrants].
func ValidateLive(dialect string, desired *schemamodel.Database, current *catalog.Database) error {
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return nil
	}
	if desired == nil || current == nil {
		return nil
	}

	managed := make(map[string]bool, len(desired.Roles))
	for _, role := range desired.Roles {
		managed[role.Name] = true
	}

	partial := slices.DeleteFunc(slices.Clone(current.Grants), func(grant catalog.Grant) bool {
		return !grant.IsPartialRevoke || !managed[grant.Role]
	})
	slices.SortFunc(partial, func(a, b catalog.Grant) int {
		return cmp.Or(
			cmp.Compare(a.Role, b.Role),
			cmp.Compare(a.Privilege, b.Privilege),
			cmp.Compare(a.Schema, b.Schema),
			cmp.Compare(a.ObjectName, b.ObjectName),
		)
	})

	problems := make([]error, 0, len(partial))
	for _, grant := range partial {
		problems = append(problems, fmt.Errorf(
			"role %q carries a partial revoke of %s on %s: Ptah manages ClickHouse grants, not grants with exceptions, "+
				"so it cannot describe this role's effective privileges — remove the partial revoke, or stop declaring the role",
			grant.Role, grant.Privilege, ScopeOfLive(grant).Describe()))
	}
	problems = append(problems, revokesUnderDatabaseGrants(desired, current, managed)...)
	return errors.Join(problems...)
}

// revokesUnderDatabaseGrants refuses a declared revoke on a table where the
// role keeps a grant of the same privilege, or of any privilege for ALL, on the
// table's whole database.
//
// A grant on db.* covers every table in db, and the catalog reports it once, on
// the database. A revoke on db.t therefore finds no row on the table to take:
// measured on ClickHouse 26.10, after `GRANT SELECT ON db.* TO r` the plan for
// `REVOKE ALL ON t FROM r` revokes nothing, a member keeps reading t, and the
// next plan reads as synced. Taking t out of the database grant would leave a
// partial revoke, which Ptah does not model.
//
// A database grant the plan removes does not keep the table: an undeclared
// grant on a role the schema manages. A grant on `*.*` is outside the read, so
// it is not seen here.
func revokesUnderDatabaseGrants(desired *schemamodel.Database, current *catalog.Database, managed map[string]bool) []error {
	var problems []error
	for _, revoke := range desired.RevokedGrants {
		if revoke.OnTable == "" || revoke.OnSchema != "" {
			continue
		}
		for _, grant := range current.Grants {
			if !keepsTable(revoke, grant) || (managed[grant.Role] && !declaresOnDatabase(desired.Grants, grant)) {
				continue
			}
			table := revoke.OnTable
			if !strings.Contains(table, ".") {
				table = grant.ObjectName + "." + table
			}
			problems = append(problems, fmt.Errorf(
				"role %q holds %s on %s.*, which covers %s, so a revoke on the table cannot take it: "+
					"ClickHouse keeps the database grant and Ptah does not model a partial revoke — "+
					"revoke %s on %s.* instead, or grant it on each table that needs it",
				grant.Role, grant.Privilege, grant.ObjectName, table, grant.Privilege, grant.ObjectName))
		}
	}
	return problems
}

// keepsTable reports whether live is a grant on a whole database that gives
// the role of revoke a privilege revoke takes on a table of that database. An
// unqualified table is in the connected database, the only one the catalog
// reads grants for.
func keepsTable(revoke schemamodel.Grant, live catalog.Grant) bool {
	database, _, qualified := strings.Cut(revoke.OnTable, ".")
	return strings.EqualFold(strings.TrimSpace(live.ObjectType), "SCHEMA") && !live.IsPartialRevoke &&
		live.Role == revoke.Role && (!qualified || live.ObjectName == database) &&
		revokeNames(revoke.Privileges, live.Privilege)
}

// revokeNames reports whether a revoke of privileges takes privilege.
func revokeNames(privileges []string, privilege string) bool {
	return slices.ContainsFunc(privileges, func(name string) bool {
		name = strings.ToUpper(strings.TrimSpace(name))
		return name == "ALL" || name == "ALL PRIVILEGES" || name == privilege
	})
}

// declaresOnDatabase reports whether a declared grant gives the live
// database-scope grant's privilege, or ALL, to its role on its database.
func declaresOnDatabase(grants []schemamodel.Grant, live catalog.Grant) bool {
	return slices.ContainsFunc(grants, func(grant schemamodel.Grant) bool {
		return grant.Role == live.Role && grant.OnSchema == live.ObjectName && grant.OnTable == "" &&
			revokeNames(grant.Privileges, live.Privilege)
	})
}
