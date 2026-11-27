package mysql_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
)

// Both an addition and a rollback's replacement must wait for the relations
// their body reads. SQL Server resolves schema-bound bodies at creation, and
// Oracle records an invalid routine when the relation does not exist.
func TestPlan_RoutineCreationsFollowTheirRelations(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, platform.SQLServer, platform.Oracle} {
		for _, change := range []string{"added", "replaced"} {
			t.Run(dialect+"/"+change, func(t *testing.T) {
				c := qt.New(t)
				diff := routineOrderDiff(dialect, schemamodel.Field{Name: "n", Type: "INTEGER"})
				diff.ViewsAdded = difftypes.ViewChanges{{Name: "totals", Body: "SELECT n FROM source_rows"}}
				fn := schemamodel.Function{Name: "read_total", Language: routineOrderLanguage(dialect), Returns: "INTEGER",
					Body: "BEGIN RETURN (SELECT n FROM totals); END;"}
				setOrderedRoutine(diff, fn, change)

				nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, dialect)

				c.Assert(err, qt.IsNil)
				order := routineCreationKeys(nodes)
				c.Assert(order, qt.DeepEquals, []string{"table:target_rows", "table:source_rows", "view:totals", "routine:read_total"})
			})
		}
	}
}

// A table expression can require the opposite edge. In particular, moving
// every routine after every relation cannot create source -> view -> routine
// -> table, or the next view that calls the same routine.
func TestPlan_RoutinesStayBeforeTheTableExpressionsThatCallThem(t *testing.T) {
	for _, row := range []struct {
		name  string
		field schemamodel.Field
	}{
		{name: "default", field: schemamodel.Field{Name: "n", Type: "INTEGER", DefaultExpr: "read_total()"}},
		{name: "check", field: schemamodel.Field{Name: "n", Type: "INTEGER", Check: "n >= read_total()"}},
		{name: "generated", field: schemamodel.Field{Name: "n", Type: "INTEGER", GeneratedExpression: "read_total()", GeneratedKind: "STORED"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := routineOrderDiff(platform.SQLServer, row.field)
			diff.ViewsAdded = difftypes.ViewChanges{
				{Name: "summary", Body: "SELECT read_total() AS n FROM target_rows"},
				{Name: "totals", Body: "SELECT n FROM source_rows"},
			}
			setOrderedRoutine(diff, schemamodel.Function{Name: "read_total", Language: "sql", Returns: "INTEGER",
				Body: "BEGIN RETURN (SELECT n FROM totals); END;"}, "added")

			nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{
				"table:source_rows", "view:totals", "routine:read_total", "table:target_rows", "view:summary",
			})
		})
	}
}

func TestPlan_RoutineCreationCycleIsRefusedBeforeReturningSQL(t *testing.T) {
	for _, row := range []struct {
		name string
		body string
	}{
		{name: "direct read", body: "BEGIN RETURN (SELECT n FROM target_rows); END;"},
		{name: "read after a common table expression has ended", body: "BEGIN WITH target_rows AS (SELECT n FROM source_rows) SELECT n FROM target_rows; RETURN (SELECT n FROM target_rows); END;"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := routineOrderDiff(platform.SQLServer, schemamodel.Field{Name: "n", Type: "INTEGER", DefaultExpr: "read_total()"})
			setOrderedRoutine(diff, schemamodel.Function{Name: "read_total", Language: "sql", Returns: "INTEGER", Body: row.body}, "added")

			nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.SQLServer)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*routine.*dependency.*cycle.*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// Dynamic SQL does not expose the procedure's read to the name scanner. A
// helper needed by a view must not let that opaque procedure jump the view.
func TestPlan_RoutineCreationLeavesOpaqueBodiesAfterRelations(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ViewsAdded: difftypes.ViewChanges{{Name: "totals", Body: "SELECT read_total() AS n"}},
		FunctionsAdded: difftypes.FunctionChanges{
			{Function: schemamodel.Function{Name: "reader", Kind: schemamodel.FunctionKindProcedure,
				Language: "sql", Body: "EXEC(N'SELECT n FROM totals');"}},
			{Function: schemamodel.Function{Name: "read_total", Language: "sql", Returns: "INTEGER", Body: "RETURN 1"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{"routine:read_total", "view:totals", "routine:reader"})
}

func TestPlan_RoutineCreationMatchesCodeAndObjectKinds(t *testing.T) {
	for _, row := range []struct {
		name string
		body string
	}{
		{name: "a literal and a comment name the calling table", body: "BEGIN /* target_rows */ RETURN length('target_rows'); END;"},
		{name: "a recursive routine call is not a read of the calling table", body: "BEGIN RETURN target_rows(1); END;"},
		{name: "a parameter is not a read of the calling table", body: "BEGIN RETURN target_rows; END;"},
		{name: "a modulo expression reads a parameter", body: "BEGIN RETURN target_rows % 2; END;"},
		{name: "modulo can use a variable named rowtype", body: "BEGIN DECLARE rowtype INTEGER DEFAULT 2; RETURN target_rows % rowtype; END;"},
		{name: "a declaration default is an expression", body: "BEGIN DECLARE rowtype INTEGER DEFAULT 2; DECLARE n INTEGER DEFAULT target_rows % rowtype; RETURN n; END;"},
		{name: "SELECT INTO writes a local variable", body: "BEGIN SELECT COUNT(*) INTO target_rows FROM source_rows; RETURN target_rows; END;"},
		{name: "a join condition reads a parameter", body: "BEGIN RETURN (SELECT s.n FROM source_rows s JOIN source_rows other ON target_rows = other.n); END;"},
		{name: "a common table expression hides the calling table", body: "BEGIN WITH target_rows AS (SELECT n FROM source_rows) SELECT n INTO target_rows FROM target_rows; RETURN target_rows; END;"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := routineOrderDiff(platform.MySQL, schemamodel.Field{Name: "n", Type: "INTEGER", DefaultExpr: "target_rows(1)"})
			setOrderedRoutine(diff, schemamodel.Function{Name: "target_rows", Language: "sql", Returns: "INTEGER",
				Parameters: "target_rows INTEGER", Body: row.body}, "added")

			nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.MySQL)

			c.Assert(err, qt.IsNil)
			c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{"table:source_rows", "routine:target_rows", "table:target_rows"})
		})
	}
}

func TestPlan_RoutineReplacementKeepsItsDropBesideItsCreate(t *testing.T) {
	c := qt.New(t)
	diff := routineOrderDiff(platform.MySQL, schemamodel.Field{Name: "n", Type: "INTEGER"})
	setOrderedRoutine(diff, schemamodel.Function{Name: "read_total", Language: "sql", Returns: "INTEGER",
		Body: "BEGIN RETURN (SELECT n FROM source_rows); END;"}, "replaced")

	nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.MySQL)

	c.Assert(err, qt.IsNil)
	index := slices.IndexFunc(nodes, func(node ast.Node) bool {
		drop, ok := node.(*ast.DropFunctionNode)
		return ok && drop.Name == "read_total"
	})
	c.Assert(index >= 0 && index+1 < len(nodes), qt.IsTrue)
	created, ok := nodes[index+1].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(created.Name, qt.Equals, "read_total")
	c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{"table:target_rows", "table:source_rows", "routine:read_total"})
}

// Replacement DDL names the schema the read found. Its ordering metadata
// remains keyed by the declaration's name, including its original spelling.
func TestPlan_RoutineReplacementKeepsDeclaredDependenciesInHeldSchema(t *testing.T) {
	for _, row := range []struct {
		name, caller, callee         string
		heldCaller, heldCallee       string
		createdCaller, createdCallee string
	}{
		{name: "an unqualified declaration", caller: "caller", callee: "callee",
			heldCaller: "dbo.caller", heldCallee: "dbo.callee", createdCaller: "dbo.caller", createdCallee: "dbo.callee"},
		{name: "a schema with a different spelling", caller: "sales.CALLER", callee: "sales.CALLEE",
			heldCaller: "Sales.Caller", heldCallee: "Sales.Callee", createdCaller: "Sales.CALLER", createdCallee: "Sales.CALLEE"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				FunctionsModified: []difftypes.FunctionDiff{
					{FunctionName: row.caller, CurrentName: row.heldCaller, Changes: map[string]string{"body": "changed"},
						Desired: schemamodel.Function{Name: row.caller, Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "BEGIN EXEC " + row.callee + "; END;"}},
					{FunctionName: row.callee, CurrentName: row.heldCallee, Changes: map[string]string{"body": "changed"},
						Desired: schemamodel.Function{Name: row.callee, Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "BEGIN SELECT 1; END;"}},
				},
				DeclaredFunctions: difftypes.FunctionOrdering{Dependencies: map[string][]string{row.caller: {row.callee}}},
			}

			nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{"routine:" + row.createdCallee, "routine:" + row.createdCaller})
		})
	}
}

func TestPlan_RoutineCreationKeepsAViewCycleThroughAnExistingView(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ViewsAdded: difftypes.ViewChanges{{Name: "a_v", Body: "SELECT n FROM b_v"}},
		ViewsModified: []difftypes.ViewDiff{{ViewName: "b_v", Desired: schemamodel.View{
			Name: "b_v", Body: "SELECT n FROM a_v",
		}}},
	}
	setOrderedRoutine(diff, schemamodel.Function{Name: "constant", Language: "sql", Returns: "INTEGER", Body: "RETURN 1"}, "added")

	nodes, err := planner.GenerateSchemaDiffAST(c.Context(), builtintest.Runtime(), diff, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(routineCreationKeys(nodes), qt.DeepEquals, []string{"view:a_v", "view:b_v", "routine:constant"})
}

func routineOrderDiff(dialect string, target schemamodel.Field) *difftypes.SchemaDiff {
	target.StructName = "Target"
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "target_rows", StructName: "Target"}, {Name: "source_rows", StructName: "Source"}},
		Fields: []schemamodel.Field{target, {StructName: "Source", Name: "n", Type: "INTEGER"}},
	}
	return &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect(dialect), "target_rows", "source_rows")}
}

func setOrderedRoutine(diff *difftypes.SchemaDiff, fn schemamodel.Function, change string) {
	if change == "replaced" {
		diff.FunctionsModified = []difftypes.FunctionDiff{{FunctionName: fn.Name, Desired: fn, Changes: map[string]string{"body": "changed"}}}
		return
	}
	diff.FunctionsAdded = difftypes.FunctionChanges{{Function: fn}}
}

func routineOrderLanguage(dialect string) string {
	if dialect == platform.Oracle {
		return "plsql"
	}
	return "sql"
}

func routineCreationKeys(nodes []ast.Node) []string {
	var keys []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.CreateTableNode:
			keys = append(keys, "table:"+typed.Name)
		case *ast.CreateViewNode:
			keys = append(keys, "view:"+typed.Name)
		case *ast.CreateFunctionNode:
			keys = append(keys, "routine:"+typed.Name)
		}
	}
	return keys
}
