package chplan_test

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
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

func requestFixture() featureplan.Request {
	semantics := identifier.ForDialect("clickhouse")
	builder := objectidentity.NewBuilder(semantics)
	subject := builder.Table("events")
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "old_time + INTERVAL 1 DAY"}
	after := before.Desired()
	after.TTL.Value = "new_time + INTERVAL 7 DAY"
	runtime := must.Must(builtin.New())
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	coverage := must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil))
	table := featureplan.Table{Subject: subject, Action: featureplan.AlterTable}
	table.Current.Table = catalog.Table{Name: "events", Facets: must.Must(schemaext.NewFacets(before)), Columns: []catalog.Column{{Name: "old_time", DataType: "DateTime"}}}
	table.Current.FeatureCoverage = coverage
	table.Desired.Table = schemamodel.Table{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(after))}
	table.Desired.Fields = []schemamodel.Field{{StructName: "Event", Name: "new_time", Type: "DateTime"}}
	return featureplan.Request{
		Target: "clickhouse", Identifiers: semantics, Tables: []featureplan.Table{table}, ParentKinds: []schemaext.Kind{chschema.TableKind},
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &chdiff.Table{Before: before, After: after}}},
		CommonSteps: []featureplan.CommonStep{
			{ID: plangraph.StepID{Owner: "example.org/common", Name: "add"}, Parent: subject, Effects: []plangraph.Effect{{Subject: builder.Column("events", "new_time"), Action: plangraph.Create}}, AddedColumn: ast.NewColumn("new_time", "DateTime")},
			{ID: plangraph.StepID{Owner: "example.org/common", Name: "drop"}, Parent: subject, Effects: []plangraph.Effect{{Subject: builder.Column("events", "old_time"), Action: plangraph.Drop}}},
		},
	}
}

func TestTTLPlanOrdersColumnDependenciesAndPreservesOperands(t *testing.T) {
	c := qt.New(t)
	request := requestFixture()
	runtime := must.Must(builtin.New())
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	// Every registered ClickHouse parent model accounts for the altered table.
	c.Assert(result.Parents, qt.HasLen, 3)
	c.Assert([]schemaext.Kind{result.Parents[0].Kind, result.Parents[1].Kind, result.Parents[2].Kind}, qt.DeepEquals,
		[]schemaext.Kind{chschema.TableKind, chschema.IndexKind, chschema.RowPolicyKind})
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Contributions, qt.HasLen, 1)
	feature := result.Contributions[0]
	c.Assert(feature.Steps, qt.HasLen, 1)
	c.Assert(feature.Steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(feature.Steps[0].Impact.Impact, qt.Equals, schemaext.Behavioral)
	common := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/common"}
	for _, step := range request.CommonSteps {
		common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
	}
	plan, err := plangraph.Schedule(t.Context(), common, feature)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 3)
	c.Assert(plan.Steps[0].ID, qt.Equals, request.CommonSteps[0].ID)
	c.Assert(plan.Steps[2].ID, qt.Equals, request.CommonSteps[1].ID)
	op := plan.Steps[1].Payload.Payload.(*chast.AlterTTL)
	c.Assert(op.Change.Before.TTL, qt.Equals, "old_time + INTERVAL 1 DAY")
	op.Change.After.TTL.Value = "mutated"
	c.Assert(request.Changes[0].Value.(*chdiff.Table).After.TTL.Value, qt.Equals, "new_time + INTERVAL 7 DAY")
}

func TestTTLPlanRefusesIncompleteOrConflictingStateWithoutOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*featureplan.Request)
		want   string
	}{
		{"missing coverage", func(r *featureplan.Request) { r.Tables[0].Current.FeatureCoverage = schemaext.Coverage{} }, "complete captured observation"},
		{"removed parent", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.DropTable }, "surviving table"},
		{"other storage change", func(r *featureplan.Request) {
			r.Changes[0].Value.(*chdiff.Table).After.Engine.Value = "ReplacingMergeTree"
		}, "separate storage plan"},
		{"unknown effects", func(r *featureplan.Request) { r.CommonSteps[0].Effects = nil }, "unknown effects"},
		{"new TTL loses column", func(r *featureplan.Request) {
			r.CommonSteps[0].Effects[0].Action = plangraph.Drop
			r.CommonSteps[0].AddedColumn = nil
		}, "cannot be scheduled"},
		{"unchanged TTL loses column", func(r *featureplan.Request) {
			r.Changes = nil
			before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "old_time + INTERVAL 1 DAY"}
			r.Tables[0].Desired.Table.Facets = must.Must(schemaext.NewFacets(before.Desired()))
		}, "retained TTL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := requestFixture()
			test.change(&request)
			result, err := (chplan.Service{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestTTLPlanCancellationHasNoOutput(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (chplan.Service{}).PlanFeatures(ctx, requestFixture())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
