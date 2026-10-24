package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// retypePlan compares a declaration that retypes items.sku to varchar(128)
// against a database holding it as varchar, with the views the caller names, and
// returns the planned statements.
func retypePlan(t *testing.T, desiredViews []schemamodel.View, liveViews []catalog.View) []string {
	t.Helper()
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "integer", Primary: true},
			{StructName: "Item", Name: "sku", Type: "VARCHAR(128)"},
		},
		Views: desiredViews,
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "items",
			Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "sku", DataType: "character varying", UDTName: "varchar", IsNullable: "NO"},
			},
		}},
		Views: liveViews,
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, database,
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil, must.Must(builtin.New()))
	if err != nil {
		t.Fatal(err)
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	return statements
}

func indexOfStatement(statements []string, fragment string) int {
	for i, statement := range statements {
		if strings.Contains(statement, fragment) {
			return i
		}
	}
	return -1
}

// TestRetypeUnderUnchangedView_DropsThenRecreatesDependents pins the plan for a
// column type change under views that are themselves unchanged.
//
// PostgreSQL answers "cannot alter type of a column used by a view or rule", so
// the views leave before the ALTER and return after it, the dependent chain in
// creation order.
func TestRetypeUnderUnchangedView_DropsThenRecreatesDependents(t *testing.T) {
	c := qt.New(t)
	views := []schemamodel.View{
		{Name: "item_skus", Body: "SELECT id, sku FROM items"},
		{Name: "item_sku_count", Body: "SELECT sku, count(*) AS n FROM item_skus GROUP BY sku"},
	}
	live := []catalog.View{
		{Name: "item_skus", Schema: "public", Body: "SELECT items.id, items.sku FROM items"},
		{Name: "item_sku_count", Schema: "public", Body: "SELECT item_skus.sku, count(*) AS n FROM item_skus GROUP BY item_skus.sku"},
	}

	statements := retypePlan(t, views, live)
	plan := strings.Join(statements, "\n")

	alter := indexOfStatement(statements, `ALTER COLUMN "sku"`)
	dropBase := indexOfStatement(statements, `DROP VIEW IF EXISTS "item_skus"`)
	dropTop := indexOfStatement(statements, `DROP VIEW IF EXISTS "item_sku_count"`)
	createBase := indexOfStatement(statements, `VIEW "item_skus" AS`)
	createTop := indexOfStatement(statements, `VIEW "item_sku_count" AS`)

	c.Assert(alter >= 0, qt.IsTrue, qt.Commentf("no ALTER COLUMN in plan:\n%s", plan))
	c.Assert(dropBase >= 0 && dropBase < alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(dropTop >= 0 && dropTop < alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(createBase > alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(createTop > createBase, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
}

// TestRetypeUnderRemovedView_DropsItBeforeTheAlter covers a live view the
// declaration no longer holds. The removal step runs after the column step, so
// without an early drop the ALTER fails on a view the plan was about to remove.
func TestRetypeUnderRemovedView_DropsItBeforeTheAlter(t *testing.T) {
	c := qt.New(t)
	live := []catalog.View{
		{Name: "old_skus", Schema: "public", Body: "SELECT items.sku FROM items"},
	}

	statements := retypePlan(t, nil, live)
	plan := strings.Join(statements, "\n")

	alter := indexOfStatement(statements, `ALTER COLUMN "sku"`)
	drop := indexOfStatement(statements, `DROP VIEW IF EXISTS "public"."old_skus"`)
	c.Assert(alter >= 0, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(drop >= 0 && drop < alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(indexOfStatement(statements, "CREATE"), qt.Equals, -1, qt.Commentf("plan:\n%s", plan))
}

// TestRetypeWithoutDependentView_LeavesThePlanAlone guards the other side: a
// retype under no view adds no view statement.
func TestRetypeWithoutDependentView_LeavesThePlanAlone(t *testing.T) {
	c := qt.New(t)
	statements := retypePlan(t, nil, nil)
	c.Assert(indexOfStatement(statements, "VIEW"), qt.Equals, -1, qt.Commentf("plan:\n%s", strings.Join(statements, "\n")))
}
