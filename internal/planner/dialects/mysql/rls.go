package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
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
func (p *Planner) planRLS(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RowLevelSecurity) {
		return result
	}
	// The declaration travels WITH the entry (stokaro/ptah#2315), so an
	// enablement no longer goes unplanned because the schema handed alongside
	// spelled its table differently.
	for _, table := range diff.RLSEnabledTablesAdded {
		result = append(result, fromschema.FromRLSEnabledTable(table))
	}
	// The policy travels WITH the entry, and so does the schema its table is
	// declared under, which is what SQL Server addresses it by
	// (stokaro/ptah#2315). An entry carrying no policy is left alone here
	// rather than refused: this tree plans row-level security for targets that
	// merely tolerate it, and the PostgreSQL planner is where the refusal is.
	for _, policy := range diff.RLSPoliciesAdded {
		if policy.Desired.Name == "" {
			continue
		}
		result = append(result, fromschema.FromRLSPolicy(
			fromschema.QualifyRLSPolicyForTarget(policy.Desired, policy.TableSchema, p.targetDialect())))
	}
	// A modified policy is planned as a drop followed by a create. T-SQL has
	// ALTER SECURITY POLICY, but it alters the state and the predicate list
	// separately, and reconstructing which of the two a diff changed would mean
	// reading the catalog's predicate spelling back into the declaration's. The
	// pair is what a replacement is here, the same answer planFunctions
	// reached for the same reason.
	for _, policy := range diff.RLSPoliciesModified {
		if policy.Desired.Name == "" {
			continue
		}
		qualified := fromschema.QualifyRLSPolicyForTarget(policy.Desired, policy.TableSchema, p.targetDialect())
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

	for _, table := range diff.RLSEnabledTablesRemoved.Names() {
		result = append(result, ast.NewAlterTableDisableRLS(table))
	}
	return result
}
