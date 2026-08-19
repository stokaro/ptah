package mssql

import (
	"regexp"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
)

// SQL Server hosts row-level security through a SECURITY POLICY, and the shape
// is not PostgreSQL's with different keywords -- it is a different object with
// a different owner of the predicate. Everything below was measured on SQL
// Server 2025 (RTM-CU8), 17.0.4075.5.
//
// The difference that decides the whole design: T-SQL has no inline predicate
// expression. `ADD FILTER PREDICATE (tenant = 1) ON dbo.t` is `Incorrect syntax
// near '('` -- the parser rejects it before any name is resolved, so this is
// not a permissions or resolution failure that a better-formed expression could
// pass. A predicate is an invocation of an inline table-valued function that
// already exists, and the name must be two-part: `fn_tenant(tenant)` is
// `Cannot schema bind security policy ... name 'fn_tenant' is invalid for
// schema binding. Names must be in two-part format`.
//
// So a PostgreSQL policy carrying `USING (tenant_id = current_setting(...))`
// has no rendering here, and the honest answer is to say so rather than to
// invent a function the author never wrote. Ptah renders the policy and
// references the predicate function; whoever declares the policy owns that
// function (stokaro/ptah#1699).
//
// The rest of what was measured, and why each matters here:
//
//   - `CREATE SECURITY POLICY IF NOT EXISTS` is `Incorrect syntax near the
//     keyword 'IF'`, the same refusal CREATE SEQUENCE gives, so a declaration
//     asking for the guard gets the sys.security_policies existence test.
//   - One table may carry only one ENABLED policy: a second is
//     `Table 'dbo.t_rls' is already referenced by the enabled security policy`.
//     Creating it with `WITH (STATE = OFF)` is accepted, and so is a second
//     enabled policy once the first is off. PostgreSQL stacks policies on a
//     table freely, which is why this renderer cannot promise that a schema
//     declaring two policies for one table applies here.
//   - `ADD BLOCK PREDICATE` takes an optional operation
//     (AFTER INSERT / AFTER UPDATE / BEFORE UPDATE / BEFORE DELETE) and is
//     accepted without one, in which case it covers all of them.
//   - The `WITH` clause is optional and `SCHEMABINDING = OFF` is accepted, but
//     both are written explicitly: STATE is the difference between a policy
//     that filters and one that sits inert, and a plan should not leave it to
//     a server default that a later release may change.

// rlsPredicateInvocation matches the one predicate form T-SQL accepts: a
// two-part (or three-part) function name followed by an argument list.
//
// It is deliberately a shape test and not a parser. Its job is to separate a
// declaration this renderer can carry from one it cannot, so that the second
// gets a sentence naming the reason instead of a statement the engine refuses.
// Anything it lets through is still the engine's to judge.
var rlsPredicateInvocation = regexp.MustCompile(`^\s*(?:\[[^\]]+\]|[A-Za-z_][\w$]*)\s*\.\s*(?:\[[^\]]+\]|[A-Za-z_][\w$]*)\s*\(`)

// VisitCreatePolicy renders a T-SQL CREATE SECURITY POLICY.
//
// A declaration whose USING expression is not a two-part function invocation is
// refused by name rather than rendered into something the engine would reject
// or, worse, accept with a different meaning. The same goes for a TO clause:
// SQL Server has no role list on a predicate -- role scoping lives inside the
// predicate function's own body -- so honoring `TO app_user` would mean
// dropping it, and a dropped TO clause is a policy that applies to everyone.
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	if r.refuses(capability.RowLevelSecurity, "RLS policies", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if !rlsPredicateInvocation.MatchString(node.UsingExpression) {
		r.w.WriteLinef("-- SQLSERVER: RLS policy %q declares a USING expression T-SQL has no form for; "+
			"a predicate must invoke a two-part inline table-valued function, so the policy is not created.",
			node.Name)
		return nil
	}
	if node.ToRoles != "" {
		r.w.WriteLinef("-- SQLSERVER: RLS policy %q declares TO %s, which a security policy has no clause for; "+
			"scope the predicate function instead. The policy is not created.", node.Name, node.ToRoles)
		return nil
	}

	target := rlsTargetIdentifier(node.Table)
	predicates := []string{
		"  ADD FILTER PREDICATE " + node.UsingExpression + " ON " + target,
	}
	if node.WithCheckExpression != "" {
		if !rlsPredicateInvocation.MatchString(node.WithCheckExpression) {
			r.w.WriteLinef("-- SQLSERVER: RLS policy %q declares a WITH CHECK expression T-SQL has no form for; "+
				"a block predicate must invoke a two-part inline table-valued function, so the policy is not created.",
				node.Name)
			return nil
		}
		block := "  ADD BLOCK PREDICATE " + node.WithCheckExpression + " ON " + target
		if operation := blockPredicateOperation(node.PolicyFor); operation != "" {
			block += " " + operation
		}
		predicates = append(predicates, block)
	}

	statement := "CREATE SECURITY POLICY " + escapeQualifiedIdentifier(node.Name) + "\n" +
		strings.Join(predicates, ",\n") + "\n  WITH (STATE = ON)"
	if !node.Replace {
		r.w.WriteLinef("%s;", statement)
		return nil
	}
	r.w.WriteLinef("IF NOT EXISTS (SELECT 1 FROM sys.security_policies sp JOIN sys.schemas sc "+
		"ON sc.schema_id = sp.schema_id WHERE sc.name = %s AND sp.name = %s)",
		escapeStringLiteral(policySchemaOrDefault(node.Name)), escapeStringLiteral(policyBareName(node.Name)))
	r.w.WriteLinef("    EXEC(%s);", escapeStringLiteral(statement))
	return nil
}

// VisitDropPolicy renders a T-SQL DROP SECURITY POLICY.
//
// `DROP SECURITY POLICY IF EXISTS` on an absent policy is accepted, so the
// guard needs no existence test of its own.
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	if r.refuses(capability.RowLevelSecurity, "DROP POLICY", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = "IF EXISTS "
	}
	r.w.WriteLinef("DROP SECURITY POLICY %s%s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

// VisitAlterTableEnableRLS names the table-level switch T-SQL does not have.
//
// PostgreSQL needs two statements -- ENABLE ROW LEVEL SECURITY on the table and
// then CREATE POLICY -- because the switch and the policy are separate objects.
// On SQL Server the policy carries `WITH (STATE = ON)` and there is nothing to
// switch on the table itself, so a declaration asking for the switch is already
// satisfied by the policy this renderer emits.
//
// It is reported rather than dropped in silence for the reason the whole
// notSupported family exists: the author wrote a statement, and a plan that
// shows neither the statement nor a word about it reads as though the
// declaration was never made.
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	if r.refuses(capability.RowLevelSecurity, "row-level security", node.Table) {
		return nil
	}
	r.w.WriteLinef("-- SQLSERVER: table %q needs no ENABLE ROW LEVEL SECURITY; "+
		"a security policy carries its own STATE and there is no table-level switch.", node.Table)
	return nil
}

// VisitAlterTableDisableRLS names the same absent switch from the other side.
//
// Disabling row-level security on SQL Server means turning the policy off --
// `ALTER SECURITY POLICY <name> WITH (STATE = OFF)`, which is accepted -- or
// dropping it. Neither is addressed by a table name alone, which is all this
// node carries, so there is nothing to render and the reason is stated.
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	if r.refuses(capability.RowLevelSecurity, "row-level security", node.Table) {
		return nil
	}
	r.w.WriteLinef("-- SQLSERVER: table %q has no row-level security switch to disable; "+
		"turn the security policy off or drop it instead.", node.Table)
	return nil
}

// blockPredicateOperation maps a declared FOR clause onto the operation a BLOCK
// predicate accepts, and returns "" for a declaration that names none of them.
//
// An empty result is not a failure: `ADD BLOCK PREDICATE ... ON <table>` with
// no operation is accepted and covers all four, which is what ALL means.
func blockPredicateOperation(policyFor string) string {
	switch strings.ToUpper(strings.TrimSpace(policyFor)) {
	case "INSERT":
		return "AFTER INSERT"
	case "UPDATE":
		return "AFTER UPDATE"
	case "DELETE":
		return "BEFORE DELETE"
	default:
		return ""
	}
}

// policySchemaOrDefault returns the schema a qualified policy name carries, or
// the schema SQL Server would resolve an unqualified one into.
func policySchemaOrDefault(name string) string {
	parts := splitQualifiedIdentifier(name)
	if len(parts) < 2 {
		return defaultSchema
	}
	return unquoteIdentifier(parts[len(parts)-2])
}

// policyBareName returns the policy's own name with any schema qualification
// and quoting removed, which is the spelling sys.security_policies stores.
func policyBareName(name string) string {
	parts := splitQualifiedIdentifier(name)
	return unquoteIdentifier(parts[len(parts)-1])
}

// rlsTargetIdentifier renders the table a predicate filters as the two-part
// name schema binding requires.
//
// This is the clause a unit test cannot get right by inspection, and it was
// wrong until the engine said so. A security policy is schema-bound, and the
// binding covers the TARGET as well as the predicate function:
// `ADD FILTER PREDICATE dbo.fn_tenant(tenant) ON [t_rls]` is
// `Cannot schema bind security policy 'dbo.p_tenant' because name 't_rls' is
// invalid for schema binding. Names must be in two-part format` on SQL Server
// 2025 (RTM-CU8), 17.0.4075.5 -- the same refusal a one-part predicate name
// draws, from the other side of the ON.
//
// A declaration naming a bare table is therefore qualified with the schema
// SQL Server would have resolved it into anyway, rather than passed through
// as written.
func rlsTargetIdentifier(table string) string {
	if len(splitQualifiedIdentifier(table)) > 1 {
		return escapeQualifiedIdentifier(table)
	}
	return escapeIdentifier(defaultSchema) + "." + escapeQualifiedIdentifier(table)
}
