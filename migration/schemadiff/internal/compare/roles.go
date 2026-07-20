package compare

import (
	"fmt"
	"sort"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Roles performs PostgreSQL role comparison between generated and database schemas.
//
// This function handles the comparison of PostgreSQL database roles, which are
// used for authentication, authorization, and access control. Roles are compared
// by name and their complete attribute definition.
//
// # Role Comparison Logic
//
// **Generated Schema Roles**:
//   - Includes all roles defined in Go struct annotations
//   - These are roles the developer intentionally created for application security
//
// **Database Schema Roles**:
//   - Includes all user-defined roles from the database
//   - Excludes system roles (pg_*, postgres) for safety
//
// # Role Modification Detection
//
// Roles are considered modified if any of the following differ:
//   - Login capability (can the role login)
//   - Password (encrypted password hash)
//   - Superuser status (administrative privileges)
//   - CreateDB capability (can create databases)
//   - CreateRole capability (can create other roles)
//   - Inherit capability (inherits privileges from granted roles)
//   - Replication capability (can initiate replication)
//
// # Example Scenarios
//
// **Role addition**:
//   - Generated schema defines "app_user" role
//   - Database doesn't have this role
//   - Result: "app_user" added to diff.RolesAdded
//
// **Role removal**:
//   - Roles are NOT automatically marked for removal for safety reasons
//   - Existing roles not defined in schema are left untouched
//   - Manual role removal should be done by DBAs when needed
//
// **Role modification**:
//   - Both have "api_user" role
//   - Generated: different login capability or privileges
//   - Result: RoleDiff added to diff.RolesModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.RolesAdded: Roles that need to be created
//   - diff.RolesRemoved: Always empty (roles are not automatically removed for safety)
//   - diff.RolesModified: Roles with attribute differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func Roles(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Build lookup maps for role comparison
	generatedRoleMap := make(map[string]goschema.Role)
	for _, role := range generated.Roles {
		generatedRoleMap[role.Name] = role
	}

	databaseRoleMap := make(map[string]types.DBRole)
	for _, role := range database.Roles {
		databaseRoleMap[role.Name] = role
	}

	// Find added roles
	for roleName := range generatedRoleMap {
		if _, exists := databaseRoleMap[roleName]; !exists {
			diff.RolesAdded = append(diff.RolesAdded, roleName)
		}
	}

	// Note: We intentionally do not automatically mark roles for removal.
	// Roles are security-sensitive objects that may be created by DBAs,
	// other applications, or infrastructure setup. Automatic removal could
	// be dangerous and break authentication/authorization.
	// If role removal is needed, it should be done explicitly by the DBA.

	// Detect role attribute modifications
	for roleName, generatedRole := range generatedRoleMap {
		if databaseRole, roleExists := databaseRoleMap[roleName]; roleExists {
			roleComparison := RoleDefinitions(generatedRole, databaseRole)
			if len(roleComparison.Changes) > 0 {
				diff.RolesModified = append(diff.RolesModified, roleComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.RolesAdded)
	sort.Strings(diff.RolesRemoved)
	sort.Slice(diff.RolesModified, func(i, j int) bool {
		return diff.RolesModified[i].RoleName < diff.RolesModified[j].RoleName
	})
}

// RoleDefinitions compares individual role definitions and returns detailed differences.
//
// This function performs attribute-by-attribute comparison of PostgreSQL role definitions,
// identifying specific changes needed to bring the database role in line with the target
// role definition. It handles all PostgreSQL role attributes including privileges and capabilities.
//
// # Comparison Attributes
//
// The function compares the following role attributes:
//   - **Login**: Whether the role can login to the database
//   - **Password**: Role password (note: actual passwords are not compared for security)
//   - **Superuser**: Whether the role has superuser privileges
//   - **CreateDB**: Whether the role can create databases
//   - **CreateRole**: Whether the role can create other roles
//   - **Inherit**: Whether the role inherits privileges from granted roles
//   - **Replication**: Whether the role can initiate streaming replication
//
// # Password Handling
//
// Password comparison is handled specially:
//   - If target role has a password and database role doesn't, it's marked as changed
//   - If target role has no password and database role has one, no change is recorded
//   - Actual password values are not compared for security reasons
//
// # Change Format
//
// Changes are recorded in "old_value -> new_value" format for clarity:
//   - Boolean attributes: "false -> true" or "true -> false"
//   - Password: "no_password -> password_set" or similar safe representation
//
// # Parameters
//
//   - generated: Target role definition from Go struct annotations
//   - database: Current role definition from database introspection
//
// # Return Value
//
// Returns a RoleDiff structure containing:
//   - RoleName: Name of the role being compared
//   - Changes: Map of attribute changes in "old -> new" format
//
// # Example Output
//
//	RoleDiff{
//		RoleName: "app_user",
//		Changes: map[string]string{
//			"login": "false -> true",
//			"createdb": "false -> true",
//			"password": "no_password -> password_set",
//		},
//	}
func RoleDefinitions(generated goschema.Role, database types.DBRole) difftypes.RoleDiff {
	roleDiff := difftypes.RoleDiff{
		RoleName: generated.Name,
		Changes:  make(map[string]string),
	}

	// Compare login capability
	if generated.Login != database.Login {
		roleDiff.Changes["login"] = fmt.Sprintf("%t -> %t", database.Login, generated.Login)
	}

	// Compare password (special handling for security)
	// We only detect if a password needs to be set, not compare actual values
	if generated.Password != "" && !database.HasPassword {
		// If target has password but database role doesn't, mark for update
		roleDiff.Changes["password"] = "password_update_required"
	}

	// Compare superuser status
	if generated.Superuser != database.Superuser {
		roleDiff.Changes["superuser"] = fmt.Sprintf("%t -> %t", database.Superuser, generated.Superuser)
	}

	// Compare createdb capability
	if generated.CreateDB != database.CreateDB {
		roleDiff.Changes["createdb"] = fmt.Sprintf("%t -> %t", database.CreateDB, generated.CreateDB)
	}

	// Compare createrole capability
	if generated.CreateRole != database.CreateRole {
		roleDiff.Changes["createrole"] = fmt.Sprintf("%t -> %t", database.CreateRole, generated.CreateRole)
	}

	// Compare inherit capability
	if generated.Inherit != database.Inherit {
		roleDiff.Changes["inherit"] = fmt.Sprintf("%t -> %t", database.Inherit, generated.Inherit)
	}

	// Compare replication capability
	if generated.Replication != database.Replication {
		roleDiff.Changes["replication"] = fmt.Sprintf("%t -> %t", database.Replication, generated.Replication)
	}

	return roleDiff
}
