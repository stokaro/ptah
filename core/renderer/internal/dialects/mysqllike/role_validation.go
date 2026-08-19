package mysqllike

import (
	"cmp"
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// ValidateDeclaredRoles refuses a MySQL-family schema whose declared roles
// Ptah cannot read or compare. Complete-schema validation and rendering share
// this check, while the visitors use the same error for direct AST rendering.
//
// When a schema declares several roles, the sentence names the lexicographically
// first of them rather than the first one parsed. Two gates answer the same
// schema -- this one, before a caller asks for SQL, and the MySQL planner, which
// sorts diff.RolesAdded and so refuses on the alphabetically first CREATE ROLE
// it renders. Naming the parse-order role here made the two disagree about the
// same schema, and moved the name whenever a declaration was reordered.
func ValidateDeclaredRoles(dialect string, caps capability.Capabilities, roles []goschema.Role) error {
	normalized := platform.NormalizeDialect(dialect)
	if (normalized != platform.MySQL && normalized != platform.MariaDB) || len(roles) == 0 {
		return nil
	}
	// The blanket refusal was right while nothing read a role back, and it is a
	// gate ahead of the renderer, so it had to learn the key at the same time:
	// leaving it unconditional would have made the preset claim a capability
	// the first gate still refused (stokaro/ptah#1762).
	if caps.Has(capability.RoleManagement) {
		return validateRoleAttributes(normalized, roles)
	}
	first := slices.MinFunc(roles, func(a, b goschema.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return unsupportedRoleError(normalized, "CREATE ROLE", first.Name)
}

// validateRoleAttributes refuses a declaration carrying an attribute a
// MySQL-family role does not have, before any statement is rendered.
//
// The renderer refuses the same declaration when it reaches VisitCreateRole.
// Both gates exist because whole-schema rendering and planning enter by
// different doors, and they name the lexicographically first offending role so
// that two answers about one schema agree.
func validateRoleAttributes(dialect string, roles []goschema.Role) error {
	offending := make([]goschema.Role, 0, len(roles))
	for _, role := range roles {
		if role.Login || role.Password != "" || role.Superuser ||
			role.CreateDB || role.CreateRole || role.Replication {
			offending = append(offending, role)
		}
	}
	if len(offending) == 0 {
		return nil
	}
	first := slices.MinFunc(offending, func(a, b goschema.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return roleAttributeError(dialect, first.Name, "LOGIN, PASSWORD or another user attribute")
}

func unsupportedRoleError(dialect, operation, name string) error {
	return fmt.Errorf(
		"%w: %s: %s %s: Ptah does not read or compare MySQL-family role state; manage roles outside Ptah for this target",
		ptaherr.ErrUnsupportedFeature,
		dialect,
		operation,
		name,
	)
}
