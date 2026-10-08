package ydbplan_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
)

func stream(name, mode string) ydbschema.ChangefeedSpec {
	return ydbschema.ChangefeedSpec{Name: name, Mode: mode, Format: "JSON"}
}

func planningRequest(c *qt.C) featureplan.Request {
	c.Helper()
	added, removed := stream("fresh", "UPDATES"), stream("gone", "UPDATES")
	before, after := stream("same", "KEYS_ONLY"), stream("same", "UPDATES")
	desired, err := schemaext.NewObjects(ydbschema.DesiredObject("", "items", added), ydbschema.DesiredObject("", "items", after))
	c.Assert(err, qt.IsNil)
	current, err := schemaext.NewObjects(ydbschema.ObservedObject("", "items", removed), ydbschema.ObservedObject("", "items", before))
	c.Assert(err, qt.IsNil)
	desiredCoverage, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	currentCoverage, err := ydbschema.ChangefeedCoverage(schemaext.Observed, nil)
	c.Assert(err, qt.IsNil)
	semantics := identifier.ForDialect("ydb")
	return featureplan.Request{Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(),
		Tables: []featureplan.Table{{Subject: objectidentity.NewBuilder(semantics).TableParts("", "items"),
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "items"}, OwnedObjects: desired, FeatureCoverage: desiredCoverage},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}, OwnedObjects: current, FeatureCoverage: currentCoverage},
		}},
		Changes: []schemaext.ChangeRecord{
			{Subject: ydbschema.ChangefeedRef("", "items", "fresh"), Value: &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: added}}},
			{Subject: ydbschema.ChangefeedRef("", "items", "gone"), Value: &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: removed}}},
			{Subject: ydbschema.ChangefeedRef("", "items", "same"), Value: &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: before}, After: &ydbschema.DesiredChangefeed{Spec: after}}},
		},
	}
}

func TestServicePreservesReceiptsAndOrdersDropsBeforeAdditions(t *testing.T) {
	c := qt.New(t)
	request := planningRequest(c)
	result, err := (ydbplan.Service{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 3)
	for i, change := range result.Changes {
		c.Assert(change.Subject, qt.Equals, request.Changes[i].Subject)
		c.Assert(change.Kind, qt.Equals, ydbdiff.ChangefeedKind)
	}
	c.Assert(result.Changes[0].Steps, qt.HasLen, 1)
	c.Assert(result.Changes[1].Steps, qt.HasLen, 1)
	c.Assert(result.Changes[2].Steps, qt.HasLen, 2)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 4)
	c.Assert(plan.Steps[0].Payload.Payload.(*ydbast.DropChangefeed).Name, qt.Equals, "gone")
	c.Assert(plan.Steps[1].Payload.Payload.(*ydbast.DropChangefeed).Name, qt.Equals, "same")
	c.Assert(plan.Steps[2].Payload.Payload.(*ydbast.AddChangefeed).Changefeed.Name, qt.Equals, "fresh")
	c.Assert(plan.Steps[3].Payload.Payload.(*ydbast.AddChangefeed).Changefeed.Name, qt.Equals, "same")
	for _, step := range plan.Steps {
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Effects[1], qt.DeepEquals, plangraph.Effect{Subject: request.Tables[0].Subject, Action: plangraph.Read})
	}
}

func TestServiceAccountsForHostRebuildWithoutDuplicateEmission(t *testing.T) {
	c := qt.New(t)
	request := planningRequest(c)
	request.Tables[0].Action = featureplan.RebuildTable
	request.ParentKinds = []schemaext.Kind{ydbschema.ChangefeedKind}
	result, err := (ydbplan.Service{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 3)
	for i, change := range result.Changes {
		c.Assert(change.Subject, qt.Equals, request.Changes[i].Subject)
		c.Assert(change.Strategy, qt.Equals, "restore through the parent rebuild")
		c.Assert(change.Steps, qt.HasLen, 0)
	}
}

func TestServiceRefusesUnknownOrContradictoryCapturedState(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Request)
	}{
		{"missing parent", func(r *featureplan.Request) { r.Tables = nil }},
		{"missing declaration", func(r *featureplan.Request) { r.Tables[0].Desired = schemacapture.TableDeclaration{} }},
		{"missing observation", func(r *featureplan.Request) { r.Tables[0].Current = schemacapture.TableObservation{} }},
		{"unknown absence", func(r *featureplan.Request) { r.Tables[0].Current.FeatureCoverage = schemaext.Coverage{} }},
		{"capability refused", func(r *featureplan.Request) { r.Capabilities = nil }},
		{"duplicate change", func(r *featureplan.Request) { r.Changes = append(r.Changes, r.Changes[0]) }},
		{"duplicate capture", func(r *featureplan.Request) { r.Tables = append(r.Tables, r.Tables[0]) }},
		{"rebuild still validates", func(r *featureplan.Request) {
			r.Tables[0].Action = featureplan.RebuildTable
			r.Tables[0].Current = schemacapture.TableObservation{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planningRequest(c)
			test.edit(&request)
			result, err := (ydbplan.Service{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestServiceRefusesCanceledWork(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbplan.Service{}).PlanFeatures(ctx, planningRequest(c))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
