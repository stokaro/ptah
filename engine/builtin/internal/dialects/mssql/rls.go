package mssql

import "ptah.run/core/ast"

// SQL Server hosts row-level security through a SECURITY POLICY, which the
// owner in package mssqlschema declares, reads, compares and plans; its
// statements are rendered by package mssqlrender. The shared policy and
// switch nodes below describe PostgreSQL's model, a policy expression on one
// table and a table-level switch, and no source or reader produces them for
// this target any more. One that reaches this renderer comes from a diff
// built by hand, and is named and skipped rather than rendered into a
// security policy nobody declared.

func (r *Renderer) renderCreatePolicy(node *ast.CreatePolicyNode) error {
	r.notSupported("RLS policies", node.Name)
	return nil
}

func (r *Renderer) renderDropPolicy(node *ast.DropPolicyNode) error {
	r.notSupported("DROP POLICY", node.Name)
	return nil
}

func (r *Renderer) renderAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	r.notSupported("row-level security", node.Table)
	return nil
}

func (r *Renderer) renderAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	r.notSupported("row-level security", node.Table)
	return nil
}

func (r *Renderer) renderAlterTableForceRLS(node *ast.AlterTableForceRLSNode) error {
	r.notSupported("row-level security", node.Table)
	return nil
}
