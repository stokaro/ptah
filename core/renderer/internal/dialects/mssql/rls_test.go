package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// renderRLS renders one node through the SQL Server renderer.
func renderRLS(c *qt.C, node ast.Node) string {
	c.Helper()
	statement, err := renderer.RenderSQL(platform.SQLServer, node)
	c.Assert(err, qt.IsNil)
	return statement
}

func TestSQLServerRenderer_VisitCreatePolicy(t *testing.T) {
	t.Run("a two-part predicate invocation becomes a security policy", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("dbo.fn_tenant(tenant)")

		c.Assert(renderRLS(c, node), qt.Equals,
			"CREATE SECURITY POLICY [dbo].[p_tenant]\n"+
				"  ADD FILTER PREDICATE dbo.fn_tenant(tenant) ON [dbo].[t_rls]\n"+
				"  WITH (STATE = ON);\n")
	})

	t.Run("a WITH CHECK expression becomes a block predicate", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("dbo.fn_tenant(tenant)").
			SetWithCheckExpression("dbo.fn_tenant(tenant)").
			SetPolicyFor("UPDATE")

		c.Assert(renderRLS(c, node), qt.Equals,
			"CREATE SECURITY POLICY [dbo].[p_tenant]\n"+
				"  ADD FILTER PREDICATE dbo.fn_tenant(tenant) ON [dbo].[t_rls],\n"+
				"  ADD BLOCK PREDICATE dbo.fn_tenant(tenant) ON [dbo].[t_rls] AFTER UPDATE\n"+
				"  WITH (STATE = ON);\n")
	})

	// The refusals below are the point of the whole design, so each is pinned
	// separately: a declaration this target cannot carry must produce a
	// sentence naming the reason and NO statement. Rendering a policy that
	// drops the clause would leave the author with a rule that does not do
	// what they wrote.
	t.Run("an inline expression is refused by name, with no statement", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("tenant_id = current_tenant()")

		out := renderRLS(c, node)
		c.Assert(out, qt.Contains, "declares a USING expression T-SQL has no form for")
		c.Assert(out, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
	})

	t.Run("a one-part predicate name is refused, because schema binding needs two", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("fn_tenant(tenant)")

		out := renderRLS(c, node)
		c.Assert(out, qt.Contains, "two-part inline table-valued function")
		c.Assert(out, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
	})

	t.Run("a TO clause is refused rather than dropped", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("dbo.fn_tenant(tenant)").
			SetToRoles("app_user")

		out := renderRLS(c, node)
		c.Assert(out, qt.Contains, "declares TO app_user")
		c.Assert(out, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
	})

	t.Run("a block predicate that is not an invocation is refused", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("dbo.fn_tenant(tenant)").
			SetWithCheckExpression("tenant_id = 1")

		out := renderRLS(c, node)
		c.Assert(out, qt.Contains, "declares a WITH CHECK expression T-SQL has no form for")
		c.Assert(out, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
	})

	t.Run("a replace declaration gets the existence test, not IF NOT EXISTS", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("dbo.p_tenant", "t_rls").
			SetUsingExpression("dbo.fn_tenant(tenant)").
			SetReplace()

		out := renderRLS(c, node)
		// The engine answers `CREATE SECURITY POLICY IF NOT EXISTS` with
		// `Incorrect syntax near the keyword 'IF'`, so the guard has to be the
		// catalog test rather than the clause.
		c.Assert(out, qt.Not(qt.Contains), "CREATE SECURITY POLICY IF NOT EXISTS")
		c.Assert(out, qt.Contains, "IF NOT EXISTS (SELECT 1 FROM sys.security_policies")
		c.Assert(out, qt.Contains, "EXEC(")
	})
}

func TestSQLServerRenderer_VisitDropPolicy(t *testing.T) {
	t.Run("a guarded drop uses the clause the engine accepts", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewDropPolicy("dbo.p_tenant", "t_rls").SetIfExists()

		c.Assert(renderRLS(c, node), qt.Equals,
			"DROP SECURITY POLICY IF EXISTS [dbo].[p_tenant];\n")
	})

	t.Run("an unguarded drop carries no clause", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewDropPolicy("dbo.p_tenant", "t_rls")

		c.Assert(renderRLS(c, node), qt.Equals,
			"DROP SECURITY POLICY [dbo].[p_tenant];\n")
	})
}

func TestSQLServerRenderer_TableLevelRLSSwitch(t *testing.T) {
	// PostgreSQL needs ALTER TABLE ... ENABLE ROW LEVEL SECURITY beside the
	// policy. SQL Server has no such switch, and the declaration is answered
	// with a sentence rather than with silence: the author wrote a statement,
	// and a plan showing neither it nor a word about it reads as though they
	// never did.
	t.Run("enable names the absent switch", func(t *testing.T) {
		c := qt.New(t)

		out := renderRLS(c, ast.NewAlterTableEnableRLS("t_rls"))

		c.Assert(out, qt.Contains, "needs no ENABLE ROW LEVEL SECURITY")
		c.Assert(out, qt.Not(qt.Contains), "ALTER TABLE")
	})

	t.Run("disable points at the policy state instead", func(t *testing.T) {
		c := qt.New(t)

		out := renderRLS(c, ast.NewAlterTableDisableRLS("t_rls"))

		c.Assert(out, qt.Contains, "turn the security policy off or drop it")
		c.Assert(out, qt.Not(qt.Contains), "ALTER TABLE")
	})
}
