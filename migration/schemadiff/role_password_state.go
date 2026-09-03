package schemadiff

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
)

// ValidateRolePasswordComparison refuses a PostgreSQL-family comparison when
// it would need to decide whether to set a password from catalog data that
// could not safely establish password presence. Database-backed adapters that
// call the pure comparison entry points must invoke this validation first;
// pure offline comparisons deliberately retain their non-erroring semantics.
func ValidateRolePasswordComparison(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
) error {
	if !platform.IsPostgresFamily(dialect) || desired == nil || database == nil {
		return nil
	}
	desired = schemamodel.ScopeToDialect(desired, dialect)

	currentByName := make(map[string]catalog.Role, len(database.Roles)+len(database.RolesOutOfScope))
	for _, role := range database.Roles {
		currentByName[role.Name] = role
	}
	for _, role := range database.RolesOutOfScope {
		if _, described := currentByName[role.Name]; !described {
			currentByName[role.Name] = role
		}
	}

	passwordRoles := make([]string, 0, len(desired.Roles))
	for _, role := range desired.Roles {
		if role.Password != "" {
			passwordRoles = append(passwordRoles, role.Name)
		}
	}
	slices.Sort(passwordRoles)

	for _, name := range passwordRoles {
		current, exists := currentByName[name]
		if exists && current.PasswordState != catalog.RolePasswordAbsent &&
			current.PasswordState != catalog.RolePasswordPresent {
			return fmt.Errorf(
				"%w: cannot compare password for role %q: current password state is unknown",
				ptaherr.ErrInvalidSchemaDiff,
				name,
			)
		}
	}
	return nil
}
