package compare

import (
	"fmt"
	"sort"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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
//   - Includes all user-defined roles the reader found, described or not
//   - Excludes system roles (pg_*, postgres) for safety
//
// # Described Is Not The Same Question As Present
//
// PostgreSQL roles belong to the cluster, not to one database, so a reader
// asked to describe one schema leaves out roles that schema does not use even
// though they exist on the server (stokaro/ptah#1267). Existence is a
// different question from description, and answering it from the description
// alone reads "out of scope" as "absent": the diff plans CREATE ROLE for a
// role that is already there and the server refuses it at SQLSTATE 42710.
//
// So a role counts as present when it appears in either DBSchema.Roles or
// DBSchema.RolesOutOfScope, which together are every role the reader manages.
// Nothing else about this comparison changes: a role that exists is still
// compared attribute by attribute wherever the reader found it, so an
// ALTER ROLE the annotations ask for is still planned for a role outside the
// described scope. See stokaro/ptah#1276 for the general shape of this
// defect.
//
// The union is not every role the server has, and this comparison must not be
// described as if it were. A PostgreSQL reader excludes the reserved pg_ roles
// and the bootstrap superuser from both lists, because Ptah manages neither in
// either direction, so a reserved name reaching this function would land in
// RolesAdded and be planned as a statement the server refuses -- measured on
// PostgreSQL 17.10, `role "postgres"` gave `CREATE ROLE "postgres" ...` and
// SQLSTATE 42710, and `role "pg_monitor"` gave SQLSTATE 42939.
//
// A reserved name therefore never reaches this function: the desired schema is
// refused first, at the surfaces that accept one and can return an error
// (stokaro/ptah#1312). This comparison keeps no opinion about reserved names,
// which is why it still needs no error return. The single definition of
// "reserved" lives in [go.5x5.cz/ptah/internal/reservedrole], shared with the
// reader's own exclusion so the two cannot disagree.
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

	// Every role the reader manages, whether or not it described it. A role
	// missing from here is a role this reader would not report at all -- one
	// that does not exist, or one Ptah never manages; a role missing from
	// database.Roles alone is only a role this description does not speak
	// about.
	databaseRoleMap := make(map[string]types.DBRole, len(database.Roles)+len(database.RolesOutOfScope))
	for _, role := range database.Roles {
		databaseRoleMap[role.Name] = role
	}
	// The described entry wins when a name is in both lists. A PostgreSQL
	// reader's two lists are disjoint by construction, so this decides nothing
	// there; it decides for every other producer of a DBSchema -- a
	// hand-assembled one, a merged one, a future reader whose scoping rule
	// overlaps -- and without it the later list would silently overwrite the
	// attributes the description was read from.
	for _, role := range database.RolesOutOfScope {
		if _, described := databaseRoleMap[role.Name]; described {
			continue
		}
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
