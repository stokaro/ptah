package mssql_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mssql"
	"ptah.run/migration/schemadiff/difftypes"
)

const fixtureOwner = "example.org/fixture"

// fixtureChange asks a fixture owner to create, drop or read one object.
type fixtureChange struct {
	Owner  string
	Action plangraph.Action
}

func (*fixtureChange) Kind() schemaext.Kind { return fixtureOwner + "/change" }
func (v *fixtureChange) CloneChange() schemaext.ChangeValue {
	cloned := *v
	return &cloned
}

// fixtureOperation is the statement a fixture owner contributes.
type fixtureOperation struct {
	Owner  string
	Action plangraph.Action
}

func (*fixtureOperation) Kind() schemaext.Kind { return fixtureOwner + "/operation" }
func (v *fixtureOperation) CloneExtension() ast.ExtensionPayload {
	cloned := *v
	return &cloned
}

// fixtureRuntime plans each change as one operation of phase in the
// contribution of the owner the change names. ordered lists pairs of change
// indexes the owner orders, the first before the second; nothing else orders
// the steps, so the window each joins and the effects of other owners decide
// where it lands.
type fixtureRuntime struct {
	*engine.Runtime
	phase   featureplan.Phase
	ordered [][2]int
}

func (r fixtureRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contributions := make(map[string]*plangraph.Contribution[featureplan.Operation])
	var owners []string
	ids := make([]plangraph.StepID, len(request.Changes))
	for i, record := range request.Changes {
		change := record.Value.(*fixtureChange)
		contribution := contributions[change.Owner]
		if contribution == nil {
			contribution = &plangraph.Contribution[featureplan.Operation]{Owner: change.Owner}
			contributions[change.Owner] = contribution
			owners = append(owners, change.Owner)
		}
		id := plangraph.StepID{Owner: change.Owner, Name: fmt.Sprintf("%06d", i)}
		ids[i] = id
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &fixtureOperation{Owner: change.Owner, Action: change.Action}, Phase: r.phase},
			Effects: []plangraph.Effect{{Subject: record.Subject, Action: change.Action}}, Transaction: plangraph.TransactionAllowed,
			Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "test"},
		})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: record.Subject, Kind: record.Value.Kind(), Strategy: "test", Steps: []plangraph.StepID{id}})
	}
	for _, pair := range r.ordered {
		owner := contributions[ids[pair[0]].Owner]
		owner.Dependencies = append(owner.Dependencies, plangraph.Dependency{Before: ids[pair[0]], After: ids[pair[1]]})
	}
	for _, owner := range owners {
		result.Contributions = append(result.Contributions, *contributions[owner])
	}
	return result, nil
}

func fixtureSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).SchemaScopedParts(objectidentity.Kind(fixtureOwner+"/object"), "", name)
}

func fixtureRecord(name, owner string, action plangraph.Action) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: fixtureSubject(name), Value: &fixtureChange{Owner: owner, Action: action}}
}

// fixtureOrder names the statements of a plan the host tests read, in plan
// order.
func fixtureOrder(nodes []ast.Node) []string {
	var order []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.CreateViewNode:
			order = append(order, "create view")
		case *ast.DropViewNode:
			order = append(order, "drop view")
		case *ast.DropTableNode:
			order = append(order, "drop table")
		case *ast.GrantPrivilegeNode:
			order = append(order, "grant")
		case *ast.ExtensionStatement:
			if operation, ok := typed.Payload.(*fixtureOperation); ok {
				order = append(order, fmt.Sprintf("%s %ss", operation.Owner, operation.Action))
			}
		}
	}
	return order
}

// TestPlanner_PlacesFeatureOperations pins the windows a SQL Server plan
// gives a feature operation. A default one is created after the tables and
// before the views that may read it, and dropped after the views and before
// the tables. A dependent one names objects of every family and nothing
// common reads it, so it is created after the views and triggers and before
// the held-back column drops, indexes and grants, and dropped before any of
// those removals. In the default phase one object is created before another
// is dropped; in the dependent phase the owner orders the two.
func TestPlanner_PlacesFeatureOperations(t *testing.T) {
	created := &difftypes.SchemaDiff{
		FeatureChanges:    []schemaext.ChangeRecord{fixtureRecord("guarded", fixtureOwner, plangraph.Create)},
		ViewsAdded:        difftypes.ViewChanges{{Name: "recent", Body: "SELECT 1"}},
		DeclaredViewLikes: difftypes.ViewLikeVocabulary{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1"}}},
		GrantsAdded:       []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders"}},
	}
	dropped := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{fixtureRecord("guarded", fixtureOwner, plangraph.Drop)},
		ViewsRemoved:   difftypes.ViewChanges{{Name: "recent"}},
		TablesRemoved:  difftypes.TableRemovals{{Name: "legacy"}},
	}
	// The drop is the first change, so in the default phase only the windows
	// order it after the creation.
	exchanged := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		fixtureRecord("old", fixtureOwner, plangraph.Drop), fixtureRecord("new", fixtureOwner, plangraph.Create),
	}}
	tests := []struct {
		name    string
		phase   featureplan.Phase
		diff    *difftypes.SchemaDiff
		ordered [][2]int
		want    []string
	}{
		{name: "a default creation", phase: featureplan.PhaseDefault, diff: created,
			want: []string{fixtureOwner + " creates", "create view", "grant"}},
		{name: "a dependent creation", phase: featureplan.PhaseDependent, diff: created,
			want: []string{"create view", fixtureOwner + " creates", "grant"}},
		{name: "a default removal", phase: featureplan.PhaseDefault, diff: dropped,
			want: []string{"drop view", fixtureOwner + " drops", "drop table"}},
		{name: "a dependent removal", phase: featureplan.PhaseDependent, diff: dropped,
			want: []string{fixtureOwner + " drops", "drop view", "drop table"}},
		{name: "a default exchange", phase: featureplan.PhaseDefault, diff: exchanged,
			want: []string{fixtureOwner + " creates", fixtureOwner + " drops"}},
		{name: "a dependent exchange the owner drops first", phase: featureplan.PhaseDependent, diff: exchanged, ordered: [][2]int{{0, 1}},
			want: []string{fixtureOwner + " drops", fixtureOwner + " creates"}},
		{name: "a dependent exchange the owner creates first", phase: featureplan.PhaseDependent, diff: exchanged, ordered: [][2]int{{1, 0}},
			want: []string{fixtureOwner + " creates", fixtureOwner + " drops"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := fixtureRuntime{Runtime: must.Must(builtin.New()), phase: test.phase, ordered: test.ordered}

			nodes, err := mssql.New().GenerateMigrationAST(context.Background(), runtime, test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(fixtureOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_OrdersOwnersByTheirEffects pins that one owner's creation of an
// object precedes another owner's read of it in the same window, although
// neither owner sees the other's steps and the reader's change comes first.
func TestPlanner_OrdersOwnersByTheirEffects(t *testing.T) {
	c := qt.New(t)
	runtime := fixtureRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDependent}
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		fixtureRecord("shared", "example.org/reader", plangraph.Read), fixtureRecord("shared", "example.org/creator", plangraph.Create),
	}}

	nodes, err := mssql.New().GenerateMigrationAST(context.Background(), runtime, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(fixtureOrder(nodes), qt.DeepEquals, []string{"example.org/creator creates", "example.org/reader reads"})
}

// TestPlanner_FailurePath_RefusesUnhostedFeatureChanges pins what the planner
// still refuses: every feature change on the other targets it plans, and the
// settings of a materialized view on SQL Server, which has no owner for them.
func TestPlanner_FailurePath_RefusesUnhostedFeatureChanges(t *testing.T) {
	c := qt.New(t)
	runtime := fixtureRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDependent}
	diff := &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "daily",
		FeatureChanges: []schemaext.ChangeRecord{fixtureRecord("daily", fixtureOwner, plangraph.Alter)}}}}

	nodes, err := mssql.New().GenerateMigrationAST(context.Background(), runtime, diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*the sqlserver planner has no feature handler for .*daily.*`)
	c.Assert(nodes, qt.IsNil)
}
