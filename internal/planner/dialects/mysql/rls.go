package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
func (p *Planner) planRLS(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	if !p.capabilities().Has(capability.RowLevelSecurity) {
		return result
	}
	for _, table := range diff.RLSEnabledTablesAdded {
		declaration := findRLSEnabledTable(generated.RLSEnabledTables, table)
		if declaration == nil {
			continue
		}
		result = append(result, fromschema.FromRLSEnabledTable(*declaration))
	}
	for _, policy := range diff.RLSPoliciesAdded {
		declaration := findRLSPolicy(generated.RLSPolicies, policy.PolicyName, policy.TableName)
		if declaration == nil {
			continue
		}
		result = append(result, fromschema.FromRLSPolicy(fromschema.QualifyRLSPolicyForTarget(*declaration, *generated, p.targetDialect())))
	}
	// A modified policy is planned as a drop followed by a create. T-SQL has
	// ALTER SECURITY POLICY, but it alters the state and the predicate list
	// separately, and reconstructing which of the two a diff changed would mean
	// reading the catalog's predicate spelling back into the declaration's. The
	// pair is what a replacement is here, the same answer planFunctions
	// reached for the same reason.
	for _, policy := range diff.RLSPoliciesModified {
		declaration := findRLSPolicy(generated.RLSPolicies, policy.PolicyName, policy.TableName)
		if declaration == nil {
			continue
		}
		qualified := fromschema.QualifyRLSPolicyForTarget(*declaration, *generated, p.targetDialect())
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
func (p *Planner) removeRLS(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
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
func findRLSPolicy(declarations []goschema.RLSPolicy, name, table string) *goschema.RLSPolicy {
	for i := range declarations {
		if declarations[i].Name == name && declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}

// findRLSEnabledTable returns the declaration that asked for row-level security
// on a table, or nil when the diff names a table nothing declared.
func findRLSEnabledTable(declarations []goschema.RLSEnabledTable, table string) *goschema.RLSEnabledTable {
	for i := range declarations {
		if declarations[i].Table == table {
			return &declarations[i]
		}
	}
	return nil
}
