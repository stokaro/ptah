package clickhouse_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/migration/schemadiff/difftypes"
)

// viewSetting is an attached setting of a materialized view.
type viewSetting struct{ Replace bool }

func (*viewSetting) Kind() schemaext.Kind                 { return "example.org/view-setting" }
func (v *viewSetting) CloneChange() schemaext.ChangeValue { return &viewSetting{Replace: v.Replace} }
func (v *viewSetting) ReplacesOwner() bool                { return v.Replace }
func (*viewSetting) CloneExtension() ast.ExtensionPayload { return &viewSetting{} }

// viewPlanner answers a feature request the way an owner of view settings
// would: an in-place step for a view the request leaves standing, and no step
// for one the common plan replaces.
type viewPlanner struct {
	*engine.Runtime
	request *featureplan.Request
}

func (r viewPlanner) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.request = request
	result := featureplan.Result{Complete: true}
	for _, change := range request.Changes {
		plan := featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "the view replacement recreates the setting"}
		replaced := slices.ContainsFunc(request.CommonSteps, func(step featureplan.CommonStep) bool {
			return slices.Contains(step.Effects, plangraph.Effect{Subject: change.Subject, Action: plangraph.Drop})
		})
		if !replaced {
			id := plangraph.StepID{Owner: "example.org/view-setting", Name: "alter"}
			result.Contributions = append(result.Contributions, plangraph.Contribution[featureplan.Operation]{Owner: id.Owner, Steps: []plangraph.Step[featureplan.Operation]{{
				ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: change.Subject, Payload: &viewSetting{}},
				Effects: []plangraph.Effect{{Subject: change.Subject, Action: plangraph.Alter}}, Transaction: plangraph.TransactionForbidden,
			}}})
			plan.Strategy, plan.Steps = "change the setting in place", []plangraph.StepID{id}
		}
		result.Changes = append(result.Changes, plan)
	}
	return result, nil
}

func viewSettingDiff(replaces bool) *difftypes.SchemaDiff {
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "daily")
	view := schemamodel.MaterializedView{Name: "daily", Body: "SELECT 1"}
	return &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
		ViewName: "daily", Changes: make(map[string]string), Desired: view,
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: &viewSetting{Replace: replaces}}},
	}}}
}

// A setting its owner changes in place reaches that owner with the view's
// name bound, and the view itself is neither dropped nor recreated.
func TestMaterializedViewSettingIsPlannedInPlaceByItsOwner(t *testing.T) {
	c := qt.New(t)
	var request featureplan.Request
	diff := viewSettingDiff(false)
	nodes, err := clickhouse.New().GenerateMigrationAST(t.Context(), viewPlanner{Runtime: must.Must(engine.New()), request: &request}, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(request.Changes, qt.DeepEquals, diff.MaterializedViewsModified[0].FeatureChanges)
	c.Assert(slices.ContainsFunc(request.CommonSteps, func(step featureplan.CommonStep) bool { return len(step.Effects) > 0 }), qt.IsFalse)
	kinds := nodeKinds(nodes)
	c.Assert([]int{kinds["*ast.DropMaterializedViewNode"], kinds["*ast.CreateMaterializedViewNode"], kinds["*ast.AlterTableNode"]}, qt.DeepEquals, []int{0, 0, 1})
	alter := nodes[slices.IndexFunc(nodes, isAlterTable)].(*ast.AlterTableNode)
	c.Assert(alter.Name, qt.Equals, "daily")
	c.Assert(alter.Operations[0].(*ast.ExtensionAlterOperation).Payload, qt.DeepEquals, &viewSetting{})
}

func isAlterTable(node ast.Node) bool {
	_, ok := node.(*ast.AlterTableNode)
	return ok
}

// A setting its owner can apply only by replacing the view makes the common
// plan drop and recreate the view, and the owner is told so through the
// effects of the step that holds the view statements.
func TestMaterializedViewSettingThatReplacesTheViewIsAReplacement(t *testing.T) {
	c := qt.New(t)
	var request featureplan.Request
	diff := viewSettingDiff(true)
	nodes, err := clickhouse.New().GenerateMigrationAST(t.Context(), viewPlanner{Runtime: must.Must(engine.New()), request: &request}, diff)
	c.Assert(err, qt.IsNil)
	subject := diff.MaterializedViewsModified[0].FeatureChanges[0].Subject
	var effects []plangraph.Effect
	for _, step := range request.CommonSteps {
		effects = append(effects, step.Effects...)
	}
	c.Assert(effects, qt.DeepEquals, []plangraph.Effect{{Subject: subject, Action: plangraph.Drop}, {Subject: subject, Action: plangraph.Create}})
	kinds := nodeKinds(nodes)
	c.Assert([]int{kinds["*ast.DropMaterializedViewNode"], kinds["*ast.CreateMaterializedViewNode"], kinds["*ast.AlterTableNode"]}, qt.DeepEquals, []int{1, 1, 0})
}
