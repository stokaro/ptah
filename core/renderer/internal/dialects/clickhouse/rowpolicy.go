package clickhouse

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
)

// ClickHouse has row policies, and the shared declaration describes them more
// closely than it describes SQL Server's: `USING` is the filter and `TO` is the
// role list, so both fields land where they were written. Everything below was
// measured on ClickHouse 26.7.3.19.
//
// The one that decides the shape of this file is a silent acceptance rather
// than a refusal. `WITH CHECK <expr>` PARSES -- the statement succeeds -- and
// the clause is then dropped: system.row_policies records the USING filter and
// nothing else. A declaration carrying a write check would apply, report
// success, and protect reads only. That is the failure mode a renderer exists
// to prevent, so the declaration is refused rather than weakened.
//
// The rest:
//
//   - `FOR` accepts ALL and SELECT and nothing else. `FOR INSERT` is
//     `Syntax error ... Expected one of: ALL, SELECT`, so a policy declared for
//     a write operation has no rendering here.
//   - `CREATE OR REPLACE ROW POLICY` is a syntax error, but
//     `CREATE ROW POLICY IF NOT EXISTS` is accepted, and
//     `ALTER ROW POLICY ... USING` modifies in place -- so a change needs no
//     drop, unlike on the MySQL family.
//   - There is no table-level switch. The policy is the whole object, as on SQL
//     Server, so a declared ENABLE ROW LEVEL SECURITY is named rather than
//     dropped in silence.
//   - `AS PERMISSIVE` is the default and is written explicitly. The declaration
//     model has no field for restrictiveness, and a policy that does not say
//     which it is would be indistinguishable from a restrictive one somebody
//     wrote by hand (stokaro/ptah#1736).

// VisitCreatePolicy renders a ClickHouse CREATE ROW POLICY.
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	if !r.caps.Has(capability.RowLevelSecurity) {
		r.notSupported("CREATE POLICY", node.Name)
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// A write check cannot be honored, and the engine will not say so: it takes
	// the clause and ignores it. Rendering the policy without the check would
	// leave the author with reads filtered and writes open, which is the
	// opposite of what a WITH CHECK declaration asks for.
	if node.WithCheckExpression != "" {
		r.w.WriteLinef("-- CLICKHOUSE: row policy %q declares WITH CHECK, which this engine parses and then "+
			"ignores; the policy is not created, because creating it would filter reads and leave writes open.",
			node.Name)
		return nil
	}
	// A policy is its filter. Without one there is nothing to render but the
	// keyword, and `USING ;` is a syntax error the engine would reject at apply
	// time rather than here.
	if strings.TrimSpace(node.UsingExpression) == "" {
		r.w.WriteLinef("-- CLICKHOUSE: row policy %q declares no USING expression, and a row policy is its "+
			"filter; the policy is not created.", node.Name)
		return nil
	}
	if !rowPolicyOperationConverges(node.PolicyFor) {
		r.w.WriteLinef("-- CLICKHOUSE: row policy %q declares FOR %s. A row policy here is a read filter and "+
			"the catalog reports every one of them the same way, so this would be planned again on every run; "+
			"declare FOR ALL or leave it unset. The policy is not created.",
			node.Name, strings.ToUpper(strings.TrimSpace(node.PolicyFor)))
		return nil
	}

	// A replacement is an ALTER, not a second CREATE. `CREATE OR REPLACE ROW
	// POLICY` is a syntax error here, and `CREATE ROW POLICY IF NOT EXISTS`
	// against a policy that already exists succeeds while changing nothing --
	// which is the worst of the three, because the plan reports success and the
	// filter stays as it was. Measured: the round trip's modification step
	// applied cleanly and left `tenant_id = 1` in place until this branch
	// existed.
	verb := "CREATE ROW POLICY IF NOT EXISTS "
	if node.Replace {
		verb = "ALTER ROW POLICY "
	}
	statement := verb + escapeIdentifier(policyBareName(node.Name)) +
		" ON " + escapeQualifiedIdentifier(node.Table) +
		" AS PERMISSIVE FOR SELECT" +
		" USING " + strings.TrimSpace(node.UsingExpression)
	if roles := strings.TrimSpace(node.ToRoles); roles != "" {
		statement += " TO " + roles
	}
	r.w.WriteLinef("%s;", statement)
	return nil
}

// VisitDropPolicy renders a ClickHouse DROP ROW POLICY.
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	if !r.caps.Has(capability.RowLevelSecurity) {
		r.notSupported("DROP POLICY", node.Name)
		return nil
	}
	guard := ""
	if node.IfExists {
		guard = "IF EXISTS "
	}
	r.w.WriteLinef("DROP ROW POLICY %s%s ON %s;", guard,
		escapeIdentifier(policyBareName(node.Name)), escapeQualifiedIdentifier(node.Table))
	return nil
}

// VisitAlterTableEnableRLS names the table-level switch ClickHouse does not
// have.
//
// PostgreSQL needs the switch beside the policy because they are separate
// objects. Here the policy is the whole object and takes effect on creation, so
// a declaration asking for the switch is already satisfied by the policy this
// renderer emits. It is reported rather than dropped, because an author who
// wrote a statement should not have to guess whether it did anything.
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	if !r.caps.Has(capability.RowLevelSecurity) {
		r.notSupported("ALTER TABLE ENABLE ROW LEVEL SECURITY", node.Table)
		return nil
	}
	r.w.WriteLinef("-- CLICKHOUSE: table %q needs no ENABLE ROW LEVEL SECURITY; a row policy takes effect "+
		"when it is created and there is no table-level switch.", node.Table)
	return nil
}

// VisitAlterTableDisableRLS names the same absent switch from the other side.
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	if !r.caps.Has(capability.RowLevelSecurity) {
		r.notSupported("ALTER TABLE DISABLE ROW LEVEL SECURITY", node.Table)
		return nil
	}
	r.w.WriteLinef("-- CLICKHOUSE: table %q has no row-level security switch to disable; "+
		"drop the row policy instead.", node.Table)
	return nil
}

// rowPolicyOperationConverges reports whether a declared FOR clause is one this
// target can carry without replanning itself forever.
//
// ClickHouse stores every row policy the same way. `FOR ALL` and `FOR SELECT`
// produce identical catalog rows, and SHOW CREATE ROW POLICY reports a policy
// created FOR ALL back as FOR SELECT -- the engine does not distinguish them,
// because a row policy here filters reads and nothing else. `FOR INSERT` is a
// syntax error outright: `Expected one of: ALL, SELECT`.
//
// So the read cannot tell which spelling was used, and it answers ALL, which is
// what an annotation without `for=` parses to. A declaration that names SELECT
// explicitly would therefore be reported as changed on every run. It is named
// rather than rendered, for the same reason a SQL Server parameter default is:
// a statement that applies cleanly and never converges is worse than one that
// was not written.
func rowPolicyOperationConverges(policyFor string) bool {
	switch strings.ToUpper(strings.TrimSpace(policyFor)) {
	case "", "ALL":
		return true
	default:
		return false
	}
}

// policyBareName returns the policy's own name with any schema qualification
// removed. A ClickHouse row policy is named inside its table's database, so a
// qualified spelling carried over from a PostgreSQL declaration has no place to
// go.
func policyBareName(name string) string {
	if index := strings.LastIndex(name, "."); index >= 0 {
		return name[index+1:]
	}
	return name
}
