package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/mssqlpolicy"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// planRLS emits real row-level-security DDL for a target in this family that
// hosts it, and nothing at all for one that does not.
//
// The capability is the switch, not the dialect name -- the same shape
// planSequences and planRoles use. MySQL, MariaDB and ClickHouse leave the key
// off and keep the named skip comment reportUnsupportedRowLevelSecurity writes;
// SQL Server turns it on and the declaration becomes a SECURITY POLICY
// (stokaro/ptah#1699).
//
// Policies are planned after tables rather than before. A policy names the
// table its predicate filters and the engine resolves that name at creation
// time, so the table has to exist first -- the opposite of a sequence, which a
// column default may reference and which therefore goes first.
func (p *Planner) planRLS(result []ast.Node, diff *difftypes.SchemaDiff, desired *schemamodel.Database) []ast.Node {
	if !p.capabilities().Has(capability.RowLevelSecurity) {
		return result
	}
	for _, table := range diff.RLSEnabledTablesAdded {
		declaration := findRLSEnabledTable(desired.RLSEnabledTables, table)
		if declaration == nil {
			continue
		}
		result = append(result, fromschema.FromRLSEnabledTable(*declaration))
	}
	for _, policy := range diff.RLSPoliciesAdded {
		declaration := findRLSPolicy(desired.RLSPolicies, policy.PolicyName, policy.TableName)
		if declaration == nil {
			continue
		}
		result = append(result, fromschema.FromRLSPolicy(fromschema.QualifyRLSPolicyForTarget(*declaration, *desired, p.targetDialect())))
	}
	// A modified policy is planned as a drop followed by a create. T-SQL has
	// ALTER SECURITY POLICY, but it alters the state and the predicate list
	// separately, and reconstructing which of the two a diff changed would mean
	// reading the catalog's predicate spelling back into the declaration's. The
	// pair is what a replacement is here, the same answer planFunctions
	// reached for the same reason.
	for _, policy := range diff.RLSPoliciesModified {
		declaration := findRLSPolicy(desired.RLSPolicies, policy.PolicyName, policy.TableName)
		if declaration == nil {
			continue
		}
		qualified := fromschema.QualifyRLSPolicyForTarget(*declaration, *desired, p.targetDialect())
		// A replacement whose create half the renderer would refuse must not
		// contribute its drop half. The pair would leave the table with no
		// row-level security at all, which is a worse answer than the
		// difference it was planned to close. The create node is still emitted
		// so the refusal is visible in the plan rather than the policy
		// silently going unchanged (stokaro/ptah#2211).
		if p.targetDialect() == platform.SQLServer &&
			mssqlpolicy.UnrenderableFor(qualified.PolicyFor, qualified.WithCheckExpression) != "" {
			result = append(result, fromschema.FromRLSPolicy(qualified))
			continue
		}
		result = append(result,
			ast.NewDropPolicy(qualified.Name, qualified.Table).SetIfExists(),
			fromschema.FromRLSPolicy(qualified))
	}
	return result
}

// removeRLS drops the policies the desired schema no longer carries.
//
// It runs before table removal, not after. A security policy holds a
// schema-bound reference to the table it filters, so the table cannot be
// dropped while the policy stands.
func (p *Planner) removeRLS(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RowLevelSecurity) {
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

// findRLSPolicy returns the declaration behind a diff entry, matched on the
// pair the diff reports rather than on the policy name alone.
//
// The pair is the identity: SQL Server lets one policy carry predicates for
// several tables, so a name on its own does not name a declaration.
func findRLSPolicy(declarations []schemamodel.RLSPolicy, name, table string) *schemamodel.RLSPolicy {
	for i := range declarations {
		if declarations[i].Name == name && declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}

// findRLSEnabledTable returns the declaration that asked for row-level security
// on a table, or nil when the diff names a table nothing declared.
func findRLSEnabledTable(declarations []schemamodel.RLSEnabledTable, table string) *schemamodel.RLSEnabledTable {
	for i := range declarations {
		if declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}
