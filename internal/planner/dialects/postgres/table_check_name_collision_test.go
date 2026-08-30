package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// checkNameCollisionSchema declares a `checks` entry whose generated name is the
// one an explicit constraint already answers to, over a different expression.
func checkNameCollisionSchema() *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "P", Name: "ptah_check_collision", Checks: []string{"price > 0"},
		}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "P", Name: "price", Type: "BIGINT"},
			{StructName: "P", Name: "stock", Type: "BIGINT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName:      "P",
			Name:            "ptah_check_collision_check",
			Type:            "CHECK",
			Table:           "ptah_check_collision",
			CheckExpression: "stock >= 0",
		}},
	}
	schemamodel.Finalize(database)
	return database
}

// TestPlanner_GenerateMigrationAST_TableCheckNameDoesNotCollide pins the planner
// to the same namer the renderer and the comparator use.
//
// The planner builds its CREATE from the declaration and adds explicit
// constraints as their own ALTER statements, so it has to leave the explicit
// name free inside the CREATE. Reached through plain FromTable it did not, and
// the two surfaces disagreed while every unit test stayed green: `schema render`
// was already correct and `schema apply` emitted the colliding pair and died on
// `ERROR: constraint "ptah_check_collision_check" for relation ... already
// exists` (SQLSTATE 42710), measured on PostgreSQL 18.6.
//
// Established by mutation: reverting the planner to FromTable reddens this and
// nothing else in the tree.
func TestPlanner_GenerateMigrationAST_TableCheckNameDoesNotCollide(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	desired := checkNameCollisionSchema()
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "ptah_check_collision"),
	}

	nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, `CONSTRAINT "ptah_check_collision_check1" CHECK (price > 0)`)
	c.Assert(strings.Count(sql, `"ptah_check_collision_check"`), qt.Equals, 0)
}

// TestPlanner_GenerateMigrationAST_TableCheckKeepsItsNameWithoutACollision is
// the control: the planner does not rename a synthesized check that collides
// with nothing.
func TestPlanner_GenerateMigrationAST_TableCheckKeepsItsNameWithoutACollision(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	desired := checkNameCollisionSchema()
	desired.Constraints[0].Name = "ptah_check_collision_stock_positive"
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "ptah_check_collision"),
	}

	nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, `CONSTRAINT "ptah_check_collision_check" CHECK (price > 0)`)
	c.Assert(sql, qt.Not(qt.Contains), "ptah_check_collision_check1")
}
