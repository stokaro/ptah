package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// TestPlanner_ExactStyleDropsRemovedRoutineBeforeItsColumn pins the order
// needed to leave CASCADE off a removed column read by a removed SQL-standard
// routine. The native style keeps its established order and spelling.
func TestPlanner_ExactStyleDropsRemovedRoutineBeforeItsColumn(t *testing.T) {
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "t",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "a"}},
		}},
		FunctionsRemoved: difftypes.FunctionChanges{{
			Function: schemamodel.Function{
				Name: "count_a", Returns: "bigint", Language: "sql",
				Body: "BEGIN ATOMIC SELECT count(a) FROM t; END",
			},
			Signature: new(""),
		}},
	}
	tests := []struct {
		name    string
		planner *postgres.Planner
		order   []string
		cascade bool
	}{
		{name: "exact", planner: postgres.New().WithStatementStyle(planext.StatementStyleExact),
			order: []string{"drop function", "drop column"}},
		{name: "native", planner: postgres.New(),
			order: []string{"drop column", "drop function"}, cascade: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

			c.Assert(err, qt.IsNil)
			c.Assert(phaseOrder(nodes), qt.DeepEquals, test.order)
			c.Assert(droppedCascades(nodes)["column t.a"], qt.Equals, test.cascade)
		})
	}
}
