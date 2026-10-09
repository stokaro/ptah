package chplan_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
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

func indexFacets(value schemaext.Value) schemaext.Facets {
	return must.Must(must.Must(schemaext.NewFacets(value)).WithTargetScope(value.Kind(), "clickhouse"))
}

func observedCoverage(subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	var kinds []schemaext.KindCoverage
	for _, model := range must.Must(builtin.New()).Codecs().Definitions() {
		if (model.Kind == chschema.TableKind || model.Kind == chschema.IndexKind) && model.Representation == schemaext.Observed {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, kinds, subjects))
}

// indexRequest captures one surviving table whose skipping index moves from
// minmax with one granule to set(100) with four, while a common operation
// modifies the column its expression reads.
func indexRequest(c *qt.C) featureplan.Request {
	c.Helper()
	semantics := identifier.ForDialect("clickhouse")
	builder := objectidentity.NewBuilder(semantics)
	table := builder.Table("events")
	index := builder.IndexParts("", "events", "idx_payload")
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	before := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	after := (&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}).Desired()
	captured := featureplan.Table{Subject: table, Action: featureplan.AlterTable}
	captured.Current.Table = catalog.Table{Name: "events", Facets: must.Must(schemaext.NewFacets(storage)),
		Columns: []catalog.Column{{Name: "id", DataType: "UInt64"}, {Name: "payload", DataType: "String"}}}
	captured.Current.Indexes = []catalog.Index{{Name: "idx_payload", TableName: "events", Columns: []string{"lower(payload)"}, Facets: indexFacets(before)}}
	captured.Current.FeatureCoverage = observedCoverage()
	captured.Desired.Table = schemamodel.Table{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}
	captured.Desired.Fields = []schemamodel.Field{{StructName: "Event", Name: "id", Type: "UInt64"}, {StructName: "Event", Name: "payload", Type: "String"}}
	captured.Desired.Indexes = []schemamodel.Index{{Name: "idx_payload", StructName: "Event", TableName: "events", Fields: []string{"lower(payload)"}, Facets: indexFacets(after)}}
	return featureplan.Request{
		Target: "clickhouse", Identifiers: semantics, Tables: []featureplan.Table{captured},
		Changes: []schemaext.ChangeRecord{{Subject: index, Value: &chdiff.Index{Before: before, After: after}}},
		CommonSteps: []featureplan.CommonStep{{
			ID: plangraph.StepID{Owner: "example.org/common", Name: "modify"}, Parent: table,
			Effects: []plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: builder.Column("events", "payload"), Action: plangraph.Alter}},
		}},
	}
}

// A settings change replaces the index: the old one goes before the column it
// reads changes, and the new one, with the captured expression and the desired
// settings, comes after.
func TestIndexSettingsPlanReplacesTheIndexAroundColumnChanges(t *testing.T) {
	c := qt.New(t)
	request := indexRequest(c)
	result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 2)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.HasLen, 2)
	c.Assert(result.Contributions, qt.HasLen, 1)
	feature := result.Contributions[0]
	for _, step := range feature.Steps {
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Impact.Impact, qt.Equals, schemaext.Behavioral)
	}
	common := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/common"}
	for _, step := range request.CommonSteps {
		common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
	}
	plan, err := plangraph.Schedule(t.Context(), common, feature)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 3)
	c.Assert(plan.Steps[0].Payload.Payload, qt.DeepEquals, &chast.DropSkippingIndex{Name: "idx_payload"})
	c.Assert(plan.Steps[1].ID, qt.Equals, request.CommonSteps[0].ID)
	c.Assert(plan.Steps[2].Payload.Payload, qt.DeepEquals, &chast.AddSkippingIndex{Name: "idx_payload", Expression: "lower(payload)", IndexType: "set(100)", Granularity: 4})
	c.Assert(plan.Steps[2].Payload.Parent, qt.Equals, request.Tables[0].Subject)
	c.Assert(request.Changes[0].Value.(*chdiff.Index).After.IndexType.Value, qt.Equals, "set(100)")
}

// A common replacement of the same index already creates it from the desired
// declaration, so the owner accounts for the change without a second
// DROP/ADD pair.
func TestIndexSettingsPlanDefersToACommonReplacement(t *testing.T) {
	c := qt.New(t)
	request := indexRequest(c)
	index := request.Changes[0].Subject
	table := request.Tables[0].Subject
	request.CommonSteps = []featureplan.CommonStep{
		{ID: plangraph.StepID{Owner: "example.org/common", Name: "drop"}, Parent: table, Effects: []plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: index, Action: plangraph.Drop}}},
		{ID: plangraph.StepID{Owner: "example.org/common", Name: "add"}, Parent: table, Effects: []plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: index, Action: plangraph.Create}}},
	}
	result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.HasLen, 0)
	c.Assert(result.Changes[0].Strategy, qt.Contains, "common index replacement")
}

func TestIndexSettingsParentReceipts(t *testing.T) {
	for _, test := range []struct {
		name   string
		action featureplan.ParentAction
		want   string
	}{
		{"surviving table", featureplan.AlterTable, "retain skipping-index settings"},
		{"removed table", featureplan.DropTable, "remove skipping-index settings with the table"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest(c)
			request.Changes = nil
			request.Tables[0].Action = test.action
			request.Tables[0].Desired.Indexes[0].Facets = indexFacets((&chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}).Desired())
			request.ParentKinds = []schemaext.Kind{chschema.IndexKind}
			result, err := (chplan.IndexService{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Parents[0].Kind, qt.Equals, chschema.IndexKind)
			c.Assert(result.Parents[0].Strategy, qt.Contains, test.want)
		})
	}
}

func TestIndexSettingsPlanRefusesIncompleteOrConflictingStateWithoutOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*featureplan.Request)
		want   string
	}{
		{"removed parent", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.DropTable }, "surviving table"},
		{"rebuilt parent", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.RebuildTable }, "surviving table"},
		{"uncaptured index", func(r *featureplan.Request) { r.Tables[0].Current.Indexes = nil }, "not captured on both sides"},
		{"missing observation", func(r *featureplan.Request) { r.Tables[0].Current.Indexes[0].Facets = schemaext.Facets{} }, "observation is missing"},
		{"knowledge limit", func(r *featureplan.Request) {
			r.Tables[0].Current.FeatureCoverage = observedCoverage(schemaext.SubjectCoverage{
				Kind: chschema.IndexKind, Subject: r.Changes[0].Subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"},
			})
		}, "knowledge limit"},
		{"stale before", func(r *featureplan.Request) {
			r.Changes[0].Value.(*chdiff.Index).Before = &chschema.ObservedIndex{IndexType: "minmax", Granularity: 2}
		}, "disagrees with captured"},
		{"stale after", func(r *featureplan.Request) {
			r.Changes[0].Value.(*chdiff.Index).After.Granularity.Value = 8
		}, "disagrees with captured"},
		{"common removal only", func(r *featureplan.Request) {
			r.CommonSteps[0].Effects = append(r.CommonSteps[0].Effects, plangraph.Effect{Subject: r.Changes[0].Subject, Action: plangraph.Drop})
		}, "removes or adds index"},
		{"dropped expression column", func(r *featureplan.Request) { r.CommonSteps[0].Effects[1].Action = plangraph.Drop }, "cannot be scheduled"},
		{"empty expression", func(r *featureplan.Request) { r.Tables[0].Current.Indexes[0].Columns = []string{" "} }, "no key expression"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest(c)
			test.change(&request)
			result, err := (chplan.IndexService{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// A comparison that lost a settings change must not pass parent assessment as
// "nothing changed": the captured sides disagree and no change covers them.
func TestIndexSettingsParentRefusesAnUnplannedDifference(t *testing.T) {
	c := qt.New(t)
	request := indexRequest(c)
	request.Changes = nil
	request.ParentKinds = []schemaext.Kind{chschema.IndexKind}
	result, err := (chplan.IndexService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "without a corresponding feature change")
	c.Assert(result.Diagnostics[0].Parent, qt.DeepEquals, new(0))
	c.Assert(result.Parents, qt.HasLen, 0)
}

func TestIndexSettingsParentRefusesARebuild(t *testing.T) {
	c := qt.New(t)
	request := indexRequest(c)
	request.Changes = nil
	request.Tables[0].Action = featureplan.RebuildTable
	request.ParentKinds = []schemaext.Kind{chschema.IndexKind}
	result, err := (chplan.IndexService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "no plan for parent action")
}

func TestIndexSettingsPlanRefusesInvalidRequests(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*featureplan.Request)
	}{
		{"other target", func(r *featureplan.Request) { r.Target = "postgres" }},
		{"other parent kind", func(r *featureplan.Request) { r.ParentKinds = []schemaext.Kind{chschema.TableKind} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest(c)
			test.change(&request)
			result, err := (chplan.IndexService{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestIndexSettingsPlanCancellationHasNoOutput(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (chplan.IndexService{}).PlanFeatures(ctx, indexRequest(c))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestIndexSettingsRegistrationOwnsBothOperations(t *testing.T) {
	c := qt.New(t)
	kinds := must.Must(builtin.New()).Codecs().Definitions()
	for _, kind := range []schemaext.Kind{chast.AddSkippingIndexKind, chast.DropSkippingIndexKind, chdiff.IndexKind} {
		c.Assert(slices.ContainsFunc(kinds, func(model schemaext.CodecIdentity) bool { return model.Kind == kind }), qt.IsTrue, qt.Commentf("%s", kind))
	}
}
