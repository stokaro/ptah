package compare

import (
	"fmt"
	"sort"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/normalize"
	"go.5x5.cz/ptah/internal/rlspolicy"
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
func RLSPolicies(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	RLSPoliciesWithSemantics(generated, database, diff, identifier.ForDialect(""), cov, nil)
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
	cov Coverage,
	policies map[string]config.PolicyExpression,
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
			policyComparison := RLSPolicyDefinitionsWithExpressions(
				generatedPolicy, databasePolicy,
				policies[checkExpressionLookupKey(generatedPolicy.Table, generatedPolicy.Name)])
			if len(policyComparison.Changes) > 0 {
				diff.RLSPoliciesModified = append(diff.RLSPoliciesModified, policyComparison)
			}
		}
	}

	// The Atlas-compatible surface omits `policy` blocks the binary it stands in
	// for refuses; a document that left one out has said nothing about it
	// (stokaro/ptah#1276).
	//
	// A declared policy is always planned: the planner emits
	// `DROP POLICY IF EXISTS` immediately followed by `CREATE POLICY`, so the
	// pair converges whether or not the read looked for policies. Nothing is
	// ever withheld here; the withheld list is still passed on rather than
	// discarded, so that a future guard change shows up as a diagnostic instead
	// of as silence.
	kept, withheld := keepPlannedPolicyAdditions(cov, diff.RLSPoliciesAdded, alwaysGuardedCreations())
	diff.RLSPoliciesAdded = kept
	cov.recordUndecidedAdditions(withheld)
	diff.RLSPoliciesRemoved = keepPlannedPolicyRemovals(cov, diff.RLSPoliciesRemoved)

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

// keepPlannedPolicyAdditions splits planned policy creations into the ones this
// comparison plans and the ones it withholds, on the same rule as
// [Coverage.keepPlannedAdditions]: the read's silence is authoritative, or the
// creation is guarded and does not need it.
//
// A policy has its own list shape rather than a bare name, so it cannot use the
// shared filter; the owning schema is taken from the policy's table, since a
// policy in a schema nobody read is not described either. The withheld entries
// are reported by policy name, which is how [keepPlannedPolicyRemovals] and the
// coverage record spell one.
func keepPlannedPolicyAdditions(
	cov Coverage,
	planned []difftypes.RLSPolicyRef,
	guarded creationGuard,
) (kept []difftypes.RLSPolicyRef, withheld []coverage.Object) {
	if planned == nil {
		return nil, nil
	}
	kept = make([]difftypes.RLSPolicyRef, 0, len(planned))
	for _, ref := range planned {
		schema, _ := qualifiedName(ref.TableName)
		if cov.PlansAddition(coverage.Policy, schema, ref.PolicyName) || guarded(ref.PolicyName) {
			kept = append(kept, ref)
			continue
		}
		withheld = append(withheld, cov.withheldAddition(
			coverage.Policy, ref.PolicyName, schema, []string{ref.PolicyName}))
	}
	return kept, withheld
}

// keepPlannedPolicyRemovals drops every policy the desired state never claimed
// to describe. A policy has its own list shape rather than a bare name, so it
// cannot use the shared filter; the owning schema is taken from the policy's
// table, since a policy in a schema nobody read is not described either.
func keepPlannedPolicyRemovals(cov Coverage, planned []difftypes.RLSPolicyRef) []difftypes.RLSPolicyRef {
	if planned == nil {
		return nil
	}
	out := make([]difftypes.RLSPolicyRef, 0, len(planned))
	for _, ref := range planned {
		schema, _ := qualifiedName(ref.TableName)
		if cov.PlansRemoval(coverage.Policy, schema, ref.PolicyName) {
			out = append(out, ref)
		}
	}
	return out
}

// declaredPolicyClauses is what the declaration says, in the server's spelling
// where the server answered.
//
// A refusal says nothing about whether the two agree, so it falls back rather
// than being read as a difference.
func declaredPolicyClauses(
	policy goschema.RLSPolicy,
	resolved config.PolicyExpression,
) (using, withCheck string) {
	if !resolved.Resolved {
		return policy.UsingExpression, policy.WithCheckExpression
	}
	return resolved.Using, resolved.WithCheck
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

	// Find tables that need RLS disabled.
	//
	// A table the description declares POLICIES for is not one of them, even
	// when the description carries no enablement of its own. The planner
	// already reads a declared policy on a new table as a request to enable
	// row-level security -- a policy on a table without it does nothing at all
	// -- so a comparator that read the same silence as a request to DISABLE
	// contradicted the plan one run earlier.
	//
	// Measured on PostgreSQL 17.11 with a document declaring three policies and
	// no `row_security` block: the first apply emitted
	// `ALTER TABLE docs ENABLE ROW LEVEL SECURITY` and the second emitted
	// `DISABLE`, leaving the policies in place and unenforced. Applying the same
	// file twice turned a security control off (stokaro/ptah#2048).
	//
	// The other end was available -- the planner could stop enabling -- and this
	// is the end that keeps the control on. A description that wants row-level
	// security off says so by not declaring policies for the table.
	declaredPolicyTables := make(map[tableIdentity]struct{}, len(generated.RLSPolicies))
	for _, policy := range generated.RLSPolicies {
		declaredPolicyTables[newQualifiedTableIdentity(policy.Table, semantics)] = struct{}{}
	}
	for identity, tableName := range dbRLSTables {
		if _, declared := genRLSTables[identity]; declared {
			continue
		}
		if _, hasPolicies := declaredPolicyTables[identity]; hasPolicies {
			continue
		}
		diff.RLSEnabledTablesRemoved = append(diff.RLSEnabledTablesRemoved, tableName)
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
	return RLSPolicyDefinitionsWithExpressions(genPolicy, dbPolicy, config.PolicyExpression{})
}

// RLSPolicyDefinitionsWithExpressions is [RLSPolicyDefinitions] told what the
// server makes of the declared clauses. An unresolved value leaves the textual
// comparison in charge, which is every offline path.
func RLSPolicyDefinitionsWithExpressions(
	genPolicy goschema.RLSPolicy,
	dbPolicy types.DBRLSPolicy,
	resolved config.PolicyExpression,
) difftypes.RLSPolicyDiff {
	policyDiff := difftypes.RLSPolicyDiff{
		PolicyName: genPolicy.Name,
		TableName:  genPolicy.Table,
		Changes:    make(map[string]string),
	}

	// Compare policy type (FOR clause), folded so an unspecified clause and an
	// explicit ALL are the same value on both sides (stokaro/ptah#2211).
	if rlspolicy.Command(genPolicy.PolicyFor) != rlspolicy.Command(dbPolicy.PolicyFor) {
		policyDiff.Changes["policy_for"] = fmt.Sprintf("%s -> %s", dbPolicy.PolicyFor, genPolicy.PolicyFor)
	}

	// Compare target roles (TO clause)
	if genPolicy.ToRoles != dbPolicy.ToRoles {
		policyDiff.Changes["to_roles"] = fmt.Sprintf("%s -> %s", dbPolicy.ToRoles, genPolicy.ToRoles)
	}

	// Compare the two clauses. A resolved entry answers outright, because the
	// declaration was put through the same server that printed the catalog's
	// form; without one the textual normalizer decides, as it did before.
	//
	// PostgreSQL stores a parse tree rather than the text it was given, and the
	// cast it inserts depends on the column's type. Measured on 17.11,
	// `owner = 'x'` is stored as `((owner)::text = 'x'::text)` over a varchar
	// column and unchanged over text, so a policy nobody had touched was
	// dropped and recreated on every run (stokaro/ptah#2049).
	declaredUsing, declaredWithCheck := declaredPolicyClauses(genPolicy, resolved)
	if normalize.Expression(declaredUsing) != normalize.Expression(dbPolicy.UsingExpression) {
		policyDiff.Changes["using_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.UsingExpression, genPolicy.UsingExpression)
	}
	if normalize.Expression(declaredWithCheck) != normalize.Expression(dbPolicy.WithCheckExpression) {
		policyDiff.Changes["with_check_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.WithCheckExpression, genPolicy.WithCheckExpression)
	}

	return policyDiff
}
