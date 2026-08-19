package clickhouse

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
	diff *types.SchemaDiff,
	generated *goschema.Database,
	caps capability.Capabilities,
) []ast.Node {
	if !caps.Has(capability.RowLevelSecurity) {
		return result
	}
	for _, table := range diff.RLSEnabledTablesAdded {
		if declaration := rlsEnabledTable(generated.RLSEnabledTables, table); declaration != nil {
			result = append(result, fromschema.FromRLSEnabledTable(*declaration))
		}
	}
	for _, policy := range diff.RLSPoliciesAdded {
		if declaration := rlsPolicy(generated.RLSPolicies, policy.PolicyName, policy.TableName); declaration != nil {
			result = append(result, fromschema.FromRLSPolicy(*declaration))
		}
	}
	for _, policy := range diff.RLSPoliciesModified {
		if declaration := rlsPolicy(generated.RLSPolicies, policy.PolicyName, policy.TableName); declaration != nil {
			result = append(result, fromschema.FromRLSPolicy(*declaration).SetReplace())
		}
	}
	return result
}

// removeRowPolicies drops the policies the desired schema no longer carries.
//
// It runs before table removal. A policy names its table, and dropping the
// table first would leave the drop naming an object that is already gone.
func removeRowPolicies(result []ast.Node, diff *types.SchemaDiff, caps capability.Capabilities) []ast.Node {
	if !caps.Has(capability.RowLevelSecurity) {
		return result
	}
	for _, policy := range diff.RLSPoliciesRemoved {
		result = append(result, ast.NewDropPolicy(policy.PolicyName, policy.TableName).SetIfExists())
	}
	for _, table := range diff.RLSEnabledTablesRemoved {
		result = append(result, ast.NewAlterTableDisableRLS(table))
	}
	return result
}

// rlsPolicy returns the declaration behind a diff entry, matched on the pair the
// diff reports rather than on the policy name alone: a row policy is named
// inside its table, and two tables may carry policies of the same name.
func rlsPolicy(declarations []goschema.RLSPolicy, name, table string) *goschema.RLSPolicy {
	for i := range declarations {
		if declarations[i].Name == name && declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}

// rlsEnabledTable returns the declaration that asked for row-level security on a
// table, or nil when the diff names a table nothing declared.
func rlsEnabledTable(declarations []goschema.RLSEnabledTable, table string) *goschema.RLSEnabledTable {
	for i := range declarations {
		if declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}
