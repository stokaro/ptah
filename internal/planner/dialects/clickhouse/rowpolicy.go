package clickhouse

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// planRowPolicies emits real row-policy DDL once the target hosts it.
//
// Policies are planned after tables, not before: a policy names the table it
// filters and ClickHouse resolves that name when the policy is created.
//
// A modified policy is ONE statement, unlike the drop-then-create pair a
// replacement needs on the MySQL family. `ALTER ROW POLICY p ON db.t USING
// <expr>` changes the filter in place, measured on 26.7.3.19, and
// `CREATE OR REPLACE ROW POLICY` is a syntax error -- so the alter is not a
// convenience here, it is the only single-statement form there is
// (stokaro/ptah#1736).
func planRowPolicies(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	caps capability.Capabilities,
) []ast.Node {
	if !caps.Has(capability.RowLevelSecurity) {
		return result
	}
	// The declaration travels WITH the entry (stokaro/ptah#2315).
	for _, table := range diff.RLSEnabledTablesAdded {
		result = append(result, fromschema.FromRLSEnabledTable(table))
	}
	// The policy travels WITH the entry (stokaro/ptah#2315).
	for _, policy := range diff.RLSPoliciesAdded {
		if policy.Desired.Name != "" {
			result = append(result, fromschema.FromRLSPolicy(policy.Desired))
		}
	}
	for _, policy := range diff.RLSPoliciesModified {
		if policy.Desired.Name != "" {
			result = append(result, fromschema.FromRLSPolicy(policy.Desired).SetReplace())
		}
	}
	return result
}

// removeRowPolicies drops the policies the desired schema no longer carries.
//
// It runs before table removal. A policy names its table, and dropping the
// table first would leave the drop naming an object that is already gone.
func removeRowPolicies(result []ast.Node, diff *difftypes.SchemaDiff, caps capability.Capabilities) []ast.Node {
	if !caps.Has(capability.RowLevelSecurity) {
		return result
	}
	for _, policy := range diff.RLSPoliciesRemoved {
		result = append(result, ast.NewDropPolicy(policy.PolicyName, policy.TableName).SetIfExists())
	}
	for _, table := range diff.RLSEnabledTablesRemoved.Names() {
		result = append(result, ast.NewAlterTableDisableRLS(table))
	}
	return result
}
