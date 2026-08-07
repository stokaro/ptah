package compare

import (
	"fmt"
	"sort"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/normalize"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// RLSPolicies performs PostgreSQL RLS policy comparison between generated and database schemas.
//
// This function handles the comparison of Row-Level Security policies, which are
// PostgreSQL-specific security features used for multi-tenant data isolation and
// fine-grained access control. Policies are matched by the table they belong to
// together with their name, and then compared on their complete definition.
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
//   - Generated schema defines "user_tenant_isolation" on "users"
//   - Database doesn't have that policy on that table
//   - Result: RLSPolicyRef added to diff.RLSPoliciesAdded
//
// **Policy removal**:
//   - Database has "old_security_policy" on "users"
//   - Generated schema doesn't define that policy on that table
//   - Result: RLSPolicyRef added to diff.RLSPoliciesRemoved
//
// **Policy modification**:
//   - Both have "tenant_isolation" on "users"
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
// Results are sorted by table and then by policy name for consistent output
// across multiple runs.
func RLSPolicies(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	RLSPoliciesWithSemantics(generated, database, diff, identifier.ForDialect(""))
}

// RLSPoliciesWithSemantics is [RLSPolicies] told which identifier rules the
// target actually has, so the two sides' spellings of the owning table resolve
// to one identity.
//
// A PostgreSQL policy name is scoped to its table, not to the schema. Measured
// on PostgreSQL 17.10: `CREATE POLICY tenant_isolation` succeeds on
// `public.alpha_orders` and again on `public.zeta_orders`, leaving two rows in
// `pg_policy`, and is refused only when repeated on the same table
// ("policy \"tenant_isolation\" for table \"alpha_orders\" already exists").
// Keying either side by the policy name alone therefore collapses distinct
// policies into one entry, and the loser is compared against the wrong
// counterpart or disappears (stokaro/ptah#1276).
//
// The table component goes through [tableMemberKey] rather than the raw string
// for the reason recorded on that type: the database reports a table's schema
// as empty wherever the engine treats it as implicit, while the desired schema
// carries it explicitly, so `alpha_orders` and `public.alpha_orders` are two
// spellings of one table. Only the MATCHING is normalized -- what each side is
// reported as stays the string that side supplied, because the planner renders
// those names.
func RLSPoliciesWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	// Build lookup maps for RLS policy comparison, keyed by the owning table
	// and the policy name together.
	generatedPolicyMap := make(map[tableMemberKey]goschema.RLSPolicy, len(generated.RLSPolicies))
	for _, rlsPolicy := range generated.RLSPolicies {
		generatedPolicyMap[newTableMemberKey(rlsPolicy.Table, rlsPolicy.Name, semantics)] = rlsPolicy
	}

	databasePolicyMap := make(map[tableMemberKey]types.DBRLSPolicy, len(database.RLSPolicies))
	for _, rlsPolicy := range database.RLSPolicies {
		databasePolicyMap[newTableMemberKey(rlsPolicy.Table, rlsPolicy.Name, semantics)] = rlsPolicy
	}

	// Find added policies (inline logic to avoid duplication detection)
	for key, generatedPolicy := range generatedPolicyMap {
		if _, exists := databasePolicyMap[key]; !exists {
			diff.RLSPoliciesAdded = append(diff.RLSPoliciesAdded, difftypes.RLSPolicyRef{
				PolicyName: generatedPolicy.Name,
				TableName:  generatedPolicy.Table,
			})
		}
	}

	// Find removed policies
	for key, dbPolicy := range databasePolicyMap {
		if _, exists := generatedPolicyMap[key]; !exists {
			policyRef := difftypes.RLSPolicyRef{
				PolicyName: dbPolicy.Name,
				TableName:  dbPolicy.Table,
			}
			diff.RLSPoliciesRemoved = append(diff.RLSPoliciesRemoved, policyRef)
		}
	}

	// Detect policy definition modifications
	for key, generatedPolicy := range generatedPolicyMap {
		if databasePolicy, policyExists := databasePolicyMap[key]; policyExists {
			policyComparison := RLSPolicyDefinitions(generatedPolicy, databasePolicy)
			if len(policyComparison.Changes) > 0 {
				diff.RLSPoliciesModified = append(diff.RLSPoliciesModified, policyComparison)
			}
		}
	}

	// Ensure consistent ordering of results. The policy name alone is not a
	// total order once two tables may share one, so the table leads.
	sortRLSPolicyRefs(diff.RLSPoliciesAdded)
	sortRLSPolicyRefs(diff.RLSPoliciesRemoved)
	sort.Slice(diff.RLSPoliciesModified, func(i, j int) bool {
		if diff.RLSPoliciesModified[i].TableName == diff.RLSPoliciesModified[j].TableName {
			return diff.RLSPoliciesModified[i].PolicyName < diff.RLSPoliciesModified[j].PolicyName
		}
		return diff.RLSPoliciesModified[i].TableName < diff.RLSPoliciesModified[j].TableName
	})
}

func sortRLSPolicyRefs(refs []difftypes.RLSPolicyRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].TableName == refs[j].TableName {
			return refs[i].PolicyName < refs[j].PolicyName
		}
		return refs[i].TableName < refs[j].TableName
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
	RLSEnabledTablesWithSemantics(generated, database, diff, identifier.ForDialect(""))
}

// RLSEnabledTablesWithSemantics is [RLSEnabledTables] told which identifier
// rules the target has.
//
// The two sides spell the table differently and could not match until they were
// normalized: the database side comes from [types.DBTable.QualifiedName], which
// carries the schema the reader found, while a declaration carries whatever the
// author wrote. `secured` and `public.secured` were two tables, so an unchanged
// schema planned `ENABLE ROW LEVEL SECURITY` on a table that already had it and
// `DISABLE` on the same table, every run.
//
// Same defect as [tableMemberKey] (stokaro/ptah#1232), in a comparator that
// keys by raw string; one of the instances collected in stokaro/ptah#1276.
//
// The reported names stay the strings each side supplied, because they are what
// the planner renders. Only the matching is normalized.
func RLSEnabledTablesWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	// Create sets for comparison
	genRLSTables := make(map[tableIdentity]string)
	for _, rlsTable := range generated.RLSEnabledTables {
		genRLSTables[newQualifiedTableIdentity(rlsTable.Table, semantics)] = rlsTable.Table
	}

	dbRLSTables := make(map[tableIdentity]string)
	for _, table := range database.Tables {
		if table.RLSEnabled {
			dbRLSTables[newTableIdentity(table.Schema, table.Name, semantics)] = table.QualifiedName()
		}
	}

	// Find tables that need RLS enabled
	for identity, tableName := range genRLSTables {
		if _, enabled := dbRLSTables[identity]; !enabled {
			diff.RLSEnabledTablesAdded = append(diff.RLSEnabledTablesAdded, tableName)
		}
	}

	// Find tables that need RLS disabled
	for identity, tableName := range dbRLSTables {
		if _, declared := genRLSTables[identity]; !declared {
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
