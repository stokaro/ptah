package postgres_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// phaseObject is the object a phase change drops, as the database holds it.
// It reads the relations reads names.
type phaseObject struct {
	reads []string
}

func (*phaseObject) Kind() schemaext.Kind { return phaseOwner + "/object" }
func (v *phaseObject) Clone() schemaext.Value {
	return &phaseObject{reads: slices.Clone(v.reads)}
}
func (v *phaseObject) Equal(other schemaext.Value) bool {
	right, ok := other.(*phaseObject)
	return ok && slices.Equal(v.reads, right.reads)
}
func (v *phaseObject) ReadsRelation(relation string) bool { return slices.Contains(v.reads, relation) }

// mutePhaseObject is an object that does not say what it reads.
type mutePhaseObject struct{}

func (*mutePhaseObject) Kind() schemaext.Kind   { return phaseOwner + "/object" }
func (*mutePhaseObject) Clone() schemaext.Value { return &mutePhaseObject{} }
func (*mutePhaseObject) Equal(other schemaext.Value) bool {
	_, ok := other.(*mutePhaseObject)
	return ok
}

// droppedCascades names, for each column and table drop of nodes, whether it
// carries CASCADE.
func droppedCascades(nodes []ast.Node) map[string]bool {
	cascades := make(map[string]bool)
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.AlterTableNode:
			for _, operation := range typed.Operations {
				if drop, ok := operation.(*ast.DropColumnOperation); ok {
					cascades["column "+typed.Name+"."+drop.ColumnName] = drop.Cascade
				}
			}
		case *ast.DropTableNode:
			for _, name := range typed.TableNames() {
				cascades["table "+name] = typed.Cascade
			}
		}
	}
	return cascades
}

// TestPlanner_ExactStyleAsksRemovedFeatureObjects_HappyPath pins how a drop in
// the exact style counts an object an owner removes in a later window. The
// object answers through [schemaext.RelationReader]: a drop relies on CASCADE
// when the object reads the dropped table. An owner whose change does not say
// whether it removes state, about an object the read did not hold or one that
// cannot answer, counts as one that reads every table.
func TestPlanner_ExactStyleAsksRemovedFeatureObjects_HappyPath(t *testing.T) {
	subject := phaseSubject("reader")
	held := func(value schemaext.Value) []schemaext.Object {
		return []schemaext.Object{{Ref: subject, Value: value}}
	}
	tests := []struct {
		name    string
		current []schemaext.Object
		want    map[string]bool
	}{
		{name: "an object the read did not hold",
			want: map[string]bool{"column orders.tenant": true, "table archive": true}},
		{name: "an object that cannot answer", current: held(&mutePhaseObject{}),
			want: map[string]bool{"column orders.tenant": true, "table archive": true}},
		{name: "an object that reads neither table", current: held(&phaseObject{reads: []string{"elsewhere"}}),
			want: map[string]bool{"column orders.tenant": false, "table archive": false}},
		{name: "an object that reads the table a column is dropped from", current: held(&phaseObject{reads: []string{"orders"}}),
			want: map[string]bool{"column orders.tenant": true, "table archive": false}},
		{name: "an object that reads the removed table", current: held(&phaseObject{reads: []string{"archive"}}),
			want: map[string]bool{"column orders.tenant": false, "table archive": true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: &phaseChange{Action: plangraph.Drop}}},
				TablesModified: []difftypes.TableDiff{{TableName: "orders", ColumnsRemoved: difftypes.ColumnChanges{{Name: "tenant"}}}},
				TablesRemoved:  difftypes.TableRemovals{{Name: "archive"}},
				Features:       difftypes.FeatureContext{CurrentObjects: must.Must(schemaext.NewObjects(test.current...))},
			}
			runtime := phaseRuntime{Runtime: must.Must(builtin.New())}
			planner := postgres.New().WithStatementStyle(planext.StatementStyleExact)

			nodes, err := planner.GenerateMigrationAST(context.Background(), runtime, diff)

			c.Assert(err, qt.IsNil)
			c.Assert(droppedCascades(nodes), qt.DeepEquals, test.want)
		})
	}
}
