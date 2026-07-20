package compare

import (
	"fmt"
	"sort"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/normalize"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// RLSPolicies performs PostgreSQL RLS policy comparison between generated and database schemas.
//
// This function handles the comparison of Row-Level Security policies, which are
// PostgreSQL-specific security features used for multi-tenant data isolation and
// fine-grained access control. Policies are compared by name and their complete definition.
//
// # RLS Policy Comparison Logic
//
// **Generated Schema Policies**:
//   - Includes all RLS policies defined in Go struct annotations
//   - These are policies the developer intentionally created for data security
//
// **Database Schema Policies**:
//   - Includes all user-defined RLS policies from the database
//   - Excludes system-generated policies (if any)
//
// # Policy Modification Detection
//
// Policies are considered modified if any of the following differ:
//   - Policy type (FOR clause: ALL, SELECT, INSERT, UPDATE, DELETE)
//   - Target roles (TO clause)
//   - USING expression for row filtering
//   - WITH CHECK expression for INSERT/UPDATE validation
//
// # Example Scenarios
//
// **Policy addition**:
//   - Generated schema defines "user_tenant_isolation" policy
//   - Database doesn't have this policy
//   - Result: "user_tenant_isolation" added to diff.RLSPoliciesAdded
//
// **Policy removal**:
//   - Database has "old_security_policy" policy
//   - Generated schema doesn't define this policy
//   - Result: "old_security_policy" added to diff.RLSPoliciesRemoved
//
// **Policy modification**:
//   - Both have "tenant_isolation" policy
//   - Generated: different USING expression or target roles
//   - Result: RLSPolicyDiff added to diff.RLSPoliciesModified
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
//   - diff.RLSPoliciesAdded: Policies that need to be created
//   - diff.RLSPoliciesRemoved: Policies that exist in database but not in target schema
//   - diff.RLSPoliciesModified: Policies with definition differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func RLSPolicies(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Build lookup maps for RLS policy comparison
	generatedPolicyMap := make(map[string]goschema.RLSPolicy)
	for _, rlsPolicy := range generated.RLSPolicies {
		generatedPolicyMap[rlsPolicy.Name] = rlsPolicy
	}

	databasePolicyMap := make(map[string]types.DBRLSPolicy)
	for _, rlsPolicy := range database.RLSPolicies {
		databasePolicyMap[rlsPolicy.Name] = rlsPolicy
	}

	// Find added policies (inline logic to avoid duplication detection)
	for policyName := range generatedPolicyMap {
		if _, exists := databasePolicyMap[policyName]; !exists {
			diff.RLSPoliciesAdded = append(diff.RLSPoliciesAdded, policyName)
		}
	}

	// Find removed policies
	for policyName, dbPolicy := range databasePolicyMap {
		if _, exists := generatedPolicyMap[policyName]; !exists {
			policyRef := difftypes.RLSPolicyRef{
				PolicyName: policyName,
				TableName:  dbPolicy.Table,
			}
			diff.RLSPoliciesRemoved = append(diff.RLSPoliciesRemoved, policyRef)
		}
	}

	// Detect policy definition modifications
	for policyName, generatedPolicy := range generatedPolicyMap {
		if databasePolicy, policyExists := databasePolicyMap[policyName]; policyExists {
			policyComparison := RLSPolicyDefinitions(generatedPolicy, databasePolicy)
			if len(policyComparison.Changes) > 0 {
				diff.RLSPoliciesModified = append(diff.RLSPoliciesModified, policyComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.RLSPoliciesAdded)
	sort.Slice(diff.RLSPoliciesRemoved, func(i, j int) bool {
		return diff.RLSPoliciesRemoved[i].PolicyName < diff.RLSPoliciesRemoved[j].PolicyName
	})
	sort.Slice(diff.RLSPoliciesModified, func(i, j int) bool {
		return diff.RLSPoliciesModified[i].PolicyName < diff.RLSPoliciesModified[j].PolicyName
	})
}

// RLSEnabledTables performs RLS enablement comparison between generated and database schemas.
//
// This function handles the comparison of RLS enablement status on tables, determining
// which tables need RLS enabled or disabled based on the target schema definition.
//
// # RLS Enablement Logic
//
// **Generated Schema RLS Tables**:
//   - Includes all tables that should have RLS enabled according to annotations
//   - These are tables the developer wants to secure with row-level policies
//
// **Database Schema RLS Tables**:
//   - Includes all tables that currently have RLS enabled in the database
//   - Determined by checking pg_class.relrowsecurity for PostgreSQL
//
// # Example Scenarios
//
// **RLS enablement**:
//   - Generated schema specifies RLS should be enabled on "users" table
//   - Database doesn't have RLS enabled on "users"
//   - Result: "users" added to diff.RLSEnabledTablesAdded
//
// **RLS disablement**:
//   - Database has RLS enabled on "unmanaged_table"
//   - Generated schema doesn't specify RLS for "unmanaged_table"
//   - Result: "unmanaged_table" added to diff.RLSEnabledTablesRemoved
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
//   - diff.RLSEnabledTablesAdded: Tables that need RLS enabled
//   - diff.RLSEnabledTablesRemoved: Tables that need RLS disabled
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func RLSEnabledTables(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create sets for comparison
	genRLSTables := make(map[string]bool)
	for _, rlsTable := range generated.RLSEnabledTables {
		genRLSTables[rlsTable.Table] = true
	}

	dbRLSTables := make(map[string]bool)
	for _, table := range database.Tables {
		if table.RLSEnabled {
			dbRLSTables[table.QualifiedName()] = true
		}
	}

	// Find tables that need RLS enabled
	for tableName := range genRLSTables {
		if !dbRLSTables[tableName] {
			diff.RLSEnabledTablesAdded = append(diff.RLSEnabledTablesAdded, tableName)
		}
	}

	// Find tables that need RLS disabled
	for tableName := range dbRLSTables {
		if !genRLSTables[tableName] {
			diff.RLSEnabledTablesRemoved = append(diff.RLSEnabledTablesRemoved, tableName)
		}
	}

	// Sort for consistent output
	sort.Strings(diff.RLSEnabledTablesAdded)
	sort.Strings(diff.RLSEnabledTablesRemoved)
}

// RLSPolicyDefinitions performs detailed comparison between generated and database RLS policy definitions.
//
// This function compares all aspects of a PostgreSQL RLS policy definition to determine
// if the policy needs to be recreated due to changes in its definition. PostgreSQL
// RLS policies typically require dropping and recreating when modified.
//
// # Policy Properties Compared
//
// The function compares the following properties:
//   - **PolicyFor**: Policy type (ALL, SELECT, INSERT, UPDATE, DELETE)
//   - **ToRoles**: Target database roles
//   - **UsingExpression**: USING clause for row filtering
//   - **WithCheckExpression**: WITH CHECK clause for INSERT/UPDATE validation
//
// # Example Scenarios
//
// **USING expression change**:
//   - Generated: "tenant_id = get_current_tenant_id()"
//   - Database: "tenant_id = current_user_id()"
//   - Result: Changes["using_expression"] = "old_expr -> new_expr"
//
// **Role change**:
//   - Generated: "app_user,admin_user"
//   - Database: "app_user"
//   - Result: Changes["to_roles"] = "app_user -> app_user,admin_user"
//
// **Policy type change**:
//   - Generated: "ALL"
//   - Database: "SELECT"
//   - Result: Changes["policy_for"] = "SELECT -> ALL"
//
// # Parameters
//
//   - genPolicy: Generated RLS policy definition from Go struct annotations
//   - dbPolicy: Current database RLS policy from introspection
//
// # Return Value
//
// Returns an RLSPolicyDiff containing:
//   - PolicyName: Name of the policy being compared
//   - TableName: Name of the table the policy applies to
//   - Changes: Map of property changes in "old -> new" format
//
// # Migration Implications
//
// Policy changes typically require:
//  1. DROP POLICY policy_name ON table_name
//  2. CREATE POLICY policy_name ON table_name with new definition
func RLSPolicyDefinitions(genPolicy goschema.RLSPolicy, dbPolicy types.DBRLSPolicy) difftypes.RLSPolicyDiff {
	policyDiff := difftypes.RLSPolicyDiff{
		PolicyName: genPolicy.Name,
		TableName:  genPolicy.Table,
		Changes:    make(map[string]string),
	}

	// Compare policy type (FOR clause)
	if genPolicy.PolicyFor != dbPolicy.PolicyFor {
		policyDiff.Changes["policy_for"] = fmt.Sprintf("%s -> %s", dbPolicy.PolicyFor, genPolicy.PolicyFor)
	}

	// Compare target roles (TO clause)
	if genPolicy.ToRoles != dbPolicy.ToRoles {
		policyDiff.Changes["to_roles"] = fmt.Sprintf("%s -> %s", dbPolicy.ToRoles, genPolicy.ToRoles)
	}

	// Compare USING expression
	if normalize.Expression(genPolicy.UsingExpression) != normalize.Expression(dbPolicy.UsingExpression) {
		policyDiff.Changes["using_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.UsingExpression, genPolicy.UsingExpression)
	}

	// Compare WITH CHECK expression
	if normalize.Expression(genPolicy.WithCheckExpression) != normalize.Expression(dbPolicy.WithCheckExpression) {
		policyDiff.Changes["with_check_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.WithCheckExpression, genPolicy.WithCheckExpression)
	}

	return policyDiff
}
