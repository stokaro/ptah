package featuretests_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func declaration(c *qt.C, streams ...ydbschema.ChangefeedSpec) *schemamodel.Database {
	c.Helper()
	var objects []schemaext.Object
	for _, stream := range streams {
		objects = append(objects, ydbschema.DesiredObject("", "items", stream))
	}
	owned, err := schemaext.NewObjects(objects...)
	c.Assert(err, qt.IsNil)
	coverage, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:         []schemamodel.Field{{Name: "id", StructName: "Item", Type: "uint64", Primary: true}},
		FeatureObjects: owned, FeatureCoverage: coverage,
	}
}

func options(c *qt.C, runtime generator.Runtime, before, after *schemamodel.Database) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	current, err := goschematodb.ToDBSchema(c.Context(), before, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	caps := capability.YDB262()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), after, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: after, CurrentSchema: current, Dialect: "ydb", Capabilities: caps}
}

func TestGeneratorUsesOwnerReversalAndReportsRecoveryLimits(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	fresh := ydbschema.ChangefeedSpec{Name: "fresh", Mode: "KEYS_ONLY", Format: "JSON"}
	gone := ydbschema.ChangefeedSpec{Name: "gone", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "reader"}}}
	retained := ydbschema.ChangefeedSpec{Name: "retained", Mode: "UPDATES", Format: "JSON", Disabled: true}
	after := declaration(c, fresh)
	// Omission preserves this inspected sibling. Explicit absence still asks
	// to remove gone; a disabled declaration would ask for unsupported creation.
	after.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{
		{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "items", "retained"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "this source does not declare the retained stream"}},
	})
	c.Assert(err, qt.IsNil)
	opts := options(c, runtime, declaration(c, gone, retained), after)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 2)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	reverse := plan.Reverse.Diff.TablesModified[0]
	c.Assert(reverse.FeatureChanges, qt.HasLen, 2)
	c.Assert(reverse.Current.OwnedObjects.Len(), qt.Equals, 2)
	object, found, err := reverse.Current.OwnedObjects.Get(ydbschema.ChangefeedRef("", "items", "retained"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(object.Value.(*ydbschema.ObservedChangefeed).Spec.Disabled, qt.IsTrue)
	sql, err := builtin.RenderSQLWithCapabilities("ydb", opts.Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "Recovery limit:")
	c.Assert(sql, qt.Contains, "original stream was dropped")
	c.Assert(sql, qt.Contains, "DROP CHANGEFEED `fresh`")
	c.Assert(sql, qt.Contains, "ADD CHANGEFEED `gone`")
	c.Assert(sql, qt.Not(qt.Contains), "CHANGEFEED `retained`")
	c.Assert(plan.PriorSchema.FeatureObjects.Equal(opts.DesiredSchema.FeatureObjects), qt.IsFalse)
	c.Assert(opts.Diff.TablesModified[0].Current.OwnedObjects.Equal(reverse.Current.OwnedObjects), qt.IsFalse)
}

func TestGeneratorProjectsImplicitStateFromTheOwner(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", TopicMinActivePartitions: 4}
	after := before.Clone()
	after.TopicMinActivePartitions, after.RetentionPeriod = 0, "PT12H"
	opts := options(c, runtime, declaration(c, before), declaration(c, after))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	reverse := plan.Reverse.Diff.TablesModified[0]
	change := reverse.FeatureChanges[0].Value.(*ydbdiff.Changefeed)
	c.Assert(change.Before.Spec.TopicMinActivePartitions, qt.Equals, uint64(4))
	object, found, err := reverse.Current.OwnedObjects.Get(reverse.FeatureChanges[0].Subject)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(object.Value.(*ydbschema.ObservedChangefeed).Spec.TopicMinActivePartitions, qt.Equals, uint64(4))
}

func TestGeneratorProjectsOnlyAcceptedColumnChanges(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	before := declaration(c, stream)
	before.Fields = append(before.Fields, schemamodel.Field{StructName: "Item", Name: "retained", Type: "string", Nullable: true})
	changed := stream.Clone()
	changed.RetentionPeriod = "PT12H"
	after := declaration(c, changed)
	after.Fields = append(after.Fields, schemamodel.Field{StructName: "Item", Name: "added", Type: "string", Nullable: true})
	opts := options(c, runtime, before, after)
	filtered, skipped := diffpolicy.ApplyForDialect(opts.Diff, diffpolicy.NewSkipSet(diffpolicy.DropColumn), "ydb")
	c.Assert(skipped, qt.HasLen, 1)
	opts.Diff, opts.Policy.AllowTableRebuild = filtered, true
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	reverse := plan.Reverse.Diff.TablesModified[0]
	var names []string
	for _, column := range reverse.Current.Table.Columns {
		names = append(names, column.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"id", "retained", "added"})
	c.Assert(reverse.ColumnsAdded, qt.HasLen, 0)
	c.Assert(reverse.ColumnsRemoved.Names(), qt.DeepEquals, []string{"added"})
	c.Assert(opts.CurrentSchema.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(opts.DesiredSchema.Fields, qt.HasLen, 2)
}

type failingReverser struct {
	*engine.Runtime
	ctx     context.Context
	request schemaext.ReversalRequest
	err     error
}

func (r *failingReverser) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	r.ctx, r.request = ctx, request
	return nil, r.err
}

func TestGeneratorPropagatesContextAndSelectedReversalFailure(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	failure := errors.New("owner transport failed")
	selected := &failingReverser{Runtime: runtime, err: failure}
	after := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	opts := options(c, selected, declaration(c), declaration(c, after))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(plan, qt.IsNil)
	c.Assert(selected.ctx, qt.Equals, t.Context())
	c.Assert(selected.request.Target, qt.Equals, "ydb")
	c.Assert(selected.request.Changes, qt.HasLen, 1)
}

func TestGeneratorRejectsMissingRuntimeBeforePlanning(t *testing.T) {
	c := qt.New(t)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(plan, qt.IsNil)
}
