package clickhouserbac

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
)

// ValidateLive refuses live ClickHouse state Ptah cannot compare against a
// declaration, and returns nil for every other dialect.
//
// There is exactly one such shape, and it is the one thing about ClickHouse
// grants that does not transfer from PostgreSQL: a PARTIAL REVOKE. Measured on
// 26.7.3.19,
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
func ValidateLive(dialect string, generated *goschema.Database, database *types.DBSchema) error {
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return nil
	}
	if generated == nil || database == nil {
		return nil
	}

	managed := make(map[string]bool, len(generated.Roles))
	for _, role := range generated.Roles {
		managed[role.Name] = true
	}

	partial := slices.DeleteFunc(slices.Clone(database.Grants), func(grant types.DBGrant) bool {
		return !grant.IsPartialRevoke || !managed[grant.Role]
	})
	slices.SortFunc(partial, func(a, b types.DBGrant) int {
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
	return errors.Join(problems...)
}
