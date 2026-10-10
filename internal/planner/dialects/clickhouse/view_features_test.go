package clickhouse_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
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

// A schedule changed beside the view's body is applied by the replacement the
// body change needs, so the reverse plan replaces the view again: its note
// says so and names the rows it cannot restore, rather than describing an
// in-place change back (stokaro/ptah#4278).
func TestReplacedViewReversalDescribesTheReplacement(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{Name: "daily", Body: "SELECT 2 AS c", Facets: must.Must(chsource.RefreshFacets("every 2 hour"))}},
		FeatureCoverage:   must.Must(chsource.RefreshCoverage()),
	}
	current := &catalog.Database{
		MatViews: []catalog.MaterializedView{{Name: "daily", Body: "SELECT 1 AS c", Facets: must.Must(must.Must(schemaext.NewFacets(
			&chschema.ObservedRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}})).WithTargetScope(chschema.RefreshKind, "clickhouse"))}},
		FeatureCoverage: must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Replaces(), qt.IsTrue)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 1)
	c.Assert(plan.Reverse.Recovery[0].Strategy, qt.Equals, "replace the materialized view with its captured definition and settings")
	c.Assert(plan.Reverse.Recovery[0].Limitations, qt.Contains, "Replacing a materialized view restores its definition and settings but not the rows it held.")
}

// A replaced view is dropped in a phase of its own ahead of the column steps,
// and created again after them, so it is gone before a column it reads
// changes. Matched by name inside the phase that recreates it, the drop had no
// order against those steps (stokaro/ptah#4278).
func TestMaterializedViewReplacementDropsBeforeColumnChanges(t *testing.T) {
	c := qt.New(t)
	var request featureplan.Request
	diff := viewSettingDiff(true)
	diff.TablesModified = []difftypes.TableDiff{capturedColumnFixture(schemamodel.Table{Name: "events"})}
	diff.TablesModified[0].ColumnsAdded = difftypes.ColumnChanges{{Name: "at", Type: "DateTime"}}

	nodes, err := clickhouse.New().GenerateMigrationAST(t.Context(), viewPlanner{Runtime: must.Must(engine.New()), request: &request}, diff)

	c.Assert(err, qt.IsNil)
	drop := slices.IndexFunc(nodes, func(node ast.Node) bool { _, ok := node.(*ast.DropMaterializedViewNode); return ok })
	column := slices.IndexFunc(nodes, isAlterTable)
	create := slices.IndexFunc(nodes, func(node ast.Node) bool { _, ok := node.(*ast.CreateMaterializedViewNode); return ok })
	c.Assert(drop >= 0 && drop < column && column < create, qt.IsTrue, qt.Commentf("drop %d, column %d, create %d", drop, column, create))
}

// A target without materialized views emits no drop, so nothing tells the
// owner the view is replaced: a setting that only a replacement can apply is
// refused by its owner rather than accounted for by a replacement that never
// happens (stokaro/ptah#4278).
func TestMaterializedViewReplacementEffectsFollowTheEmittedDrop(t *testing.T) {
	c := qt.New(t)
	var request featureplan.Request
	caps := capability.ClickHouse24()
	caps[capability.MaterializedViews] = false

	_, err := clickhouse.NewWithCapabilities(caps).GenerateMigrationAST(t.Context(), viewPlanner{Runtime: must.Must(engine.New()), request: &request}, viewSettingDiff(true))

	c.Assert(err, qt.IsNil)
	for _, step := range request.CommonSteps {
		c.Assert(step.Effects, qt.HasLen, 0, qt.Commentf("step %s", step.ID))
	}
}
