package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// droppedInOrder names the routines and tables nodes drop, in order.
func droppedInOrder(nodes []ast.Node) []string {
	var dropped []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.DropFunctionNode:
			dropped = append(dropped, "function "+typed.Name)
		case *ast.DropTableNode:
			for _, name := range typed.TableNames() {
				dropped = append(dropped, "table "+name)
			}
		}
	}
	return dropped
}

func removedTable(name string, parents ...string) difftypes.TableRemoval {
	return difftypes.TableRemoval{Name: name, Current: schemacapture.TableObservation{
		Table: catalog.Table{Name: name, Type: "BASE TABLE", Parents: parents},
	}}
}

// TestPlanner_ExactStyleDropsAChildTableBeforeItsParent pins the order the
// exact style needs without CASCADE and without a guard. A DROP TABLE of a
// partitioned parent takes its partitions, so a later drop of one reports that
// the table does not exist, and a parent of plain inheritance cannot be dropped
// while a child holds it. Both names sort the parent first, which is the order
// a table with no recorded parent keeps.
func TestPlanner_ExactStyleDropsAChildTableBeforeItsParent(t *testing.T) {
	tests := []struct {
		name    string
		removed difftypes.TableRemovals
		want    []string
	}{
		{
			name:    "a child with a recorded parent",
			removed: difftypes.TableRemovals{removedTable("a"), removedTable("b", "a")},
			want:    []string{"table b", "table a"},
		},
		{
			name:    "two levels of parents",
			removed: difftypes.TableRemovals{removedTable("a"), removedTable("b", "a"), removedTable("c", "b")},
			want:    []string{"table c", "table b", "table a"},
		},
		{
			name:    "a parent the plan keeps",
			removed: difftypes.TableRemovals{removedTable("b", "a"), removedTable("c")},
			want:    []string{"table b", "table c"},
		},
		{
			name:    "no recorded parent",
			removed: difftypes.TableRemovals{removedTable("a"), removedTable("b")},
			want:    []string{"table a", "table b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.New().WithStatementStyle(planext.StatementStyleExact)

			nodes, err := planner.GenerateMigrationAST(context.Background(), must.Must(builtin.New()), &difftypes.SchemaDiff{TablesRemoved: test.removed})

			c.Assert(err, qt.IsNil)
			c.Assert(droppedInOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_ExactStyleDropsARoutineAheadOfATableItsQuotedBodyReads pins the
// difference between the servers. CockroachDB records what a quoted body reads
// and refuses to drop the table first, and PostgreSQL records nothing for it, so
// the routine keeps the routine step there.
func TestPlanner_ExactStyleDropsARoutineAheadOfATableItsQuotedBodyReads(t *testing.T) {
	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{removedTable("t")},
		FunctionsRemoved: difftypes.FunctionChanges{{
			Function:  schemamodel.Function{Name: "count_t", Returns: "bigint", Language: "sql", Body: "SELECT count(*) FROM t"},
			Signature: new(""),
		}},
	}
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []string
	}{
		{
			name: "CockroachDB", dialect: platform.CockroachDB, caps: capability.ForDialect(platform.CockroachDB),
			want: []string{"function count_t", "table t"},
		},
		{
			name: "PostgreSQL", dialect: platform.Postgres, caps: capability.ForDialect(platform.Postgres),
			want: []string{"table t", "function count_t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.NewForDialect(test.dialect, test.caps).WithStatementStyle(planext.StatementStyleExact)

			nodes, err := planner.GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

			c.Assert(err, qt.IsNil)
			c.Assert(droppedInOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}
