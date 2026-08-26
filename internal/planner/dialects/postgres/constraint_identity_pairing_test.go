package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_AModifiedConstraintIsPairedByIdentityNotSpelling measures, at the
// level the rule lives on, that the two halves of one modified constraint are
// recognized as one object when the two sides spell its table differently.
//
// A description leaves the table unqualified and a catalog reports it with its
// schema, so the pair arrives as `widget` and `public.widget`. Paired by
// spelling they are two objects, the drop is not seen as belonging to the add,
// and the ADD is emitted first -- which PostgreSQL 17 answers with
// `relation "uq_widget_scope" already exists`, because a constraint's name
// belongs to its backing index too (stokaro/ptah#1987).
//
// The rule is a fold of the host through the target's identifier semantics.
// Until this row existed the fold was measured only from migration/planner,
// three packages up: `go test ./internal/planner/dialects/postgres/...` stayed
// green with the fold removed, so nothing here said the adapter was doing
// anything (stokaro/ptah#1663).
func TestPlanner_AModifiedConstraintIsPairedByIdentityNotSpelling(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded:   []string{"uq_widget_scope"},
		ConstraintsRemoved: []string{"uq_widget_scope"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name: "uq_widget_scope", TableName: "widget",
			Type: "UNIQUE", Columns: []string{"tenant"},
		}},
		// The catalog's spelling of the same table, which is the whole point of
		// the row: only a folded host makes these two one object.
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
			Name: "uq_widget_scope", TableName: "public.widget", Type: "UNIQUE",
		}},
		IdentifierSemantics: &semantics,
	}

	sql := renderedPlan(c, diff, widgetDeclaringScopeConstraint())

	drop := strings.Index(sql, `DROP CONSTRAINT IF EXISTS "uq_widget_scope"`)
	add := strings.Index(sql, `ADD CONSTRAINT "uq_widget_scope"`)
	c.Assert(drop, qt.Not(qt.Equals), -1, qt.Commentf("no drop planned:\n%s", sql))
	c.Assert(add, qt.Not(qt.Equals), -1, qt.Commentf("no add planned:\n%s", sql))
	c.Assert(drop < add, qt.IsTrue,
		qt.Commentf("the add precedes the drop, which the server refuses:\n%s", sql))
}

// widgetDeclaringScopeConstraint is the desired state the addition is resolved
// against, spelling the table the way a description does: without a schema.
func widgetDeclaringScopeConstraint() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []goschema.Field{
			{StructName: "Widget", Name: "tenant", Type: "int"},
			{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
		},
		Constraints: []goschema.Constraint{{
			StructName: "Widget", Name: "uq_widget_scope",
			Type: "UNIQUE", Columns: []string{"tenant"},
		}},
	}
}

func renderedPlan(c *qt.C, diff *difftypes.SchemaDiff, generated *goschema.Database) string {
	c.Helper()
	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}
