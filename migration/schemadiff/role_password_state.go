package schemadiff

import (
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// ValidateRolePasswordComparison refuses a PostgreSQL-family comparison when
// it would need to decide whether to set a password from catalog data that
// could not safely establish password presence. Database-backed adapters that
// call the pure comparison entry points must invoke this validation first;
// pure offline comparisons deliberately retain their non-erroring semantics.
func ValidateRolePasswordComparison(
	desired *schemamodel.Database,
	database *catalog.Database,
	selected schemaext.TargetSelection,
) error {
	if err := selected.Validate(); err != nil {
		return err
	}
	if !platform.IsPostgresFamily(selected.Name()) || desired == nil || database == nil {
		return nil
	}
	desired, err := schemamodel.ScopeToTarget(desired, selected)
	if err != nil {
		return err
	}

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
