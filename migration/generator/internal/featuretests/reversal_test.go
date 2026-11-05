package featuretests_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// declaration is table orders on ClickHouse, with the row policy named by
// each of policies.
func declaration(c *qt.C, policies ...string) *schemamodel.Database {
	c.Helper()
	var objects []schemaext.Object
	for _, name := range policies {
		objects = append(objects, schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", name),
			Value: &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Roles: chschema.RoleSelection{All: true}}})
	}
	owned, err := schemaext.NewObjects(objects...)
	c.Assert(err, qt.IsNil)
	coverage, err := chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Name: "orders", StructName: "Order", Engine: "MergeTree"}},
		Fields:         []schemamodel.Field{{Name: "id", StructName: "Order", Type: "UInt64", Primary: true}},
		FeatureObjects: owned, FeatureCoverage: coverage,
	}
}

// options plans after against the database before describes, on dialect.
func options(c *qt.C, runtime generator.Runtime, dialect string, before, after *schemamodel.Database) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	current, err := goschematodb.ToDBSchema(c.Context(), before, dialect, runtime)
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(dialect)
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), after, current, catalog.ServerInfo{Dialect: dialect, Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: after, CurrentSchema: current, Dialect: dialect, Capabilities: caps}
}

// TestGeneratorUsesOwnerReversalAndReportsRecoveryLimits plans, on
// ClickHouse table orders, the creation of row policy fresh and the drop of
// gone, beside retained, which the declaration leaves undescribed and so keeps.
// The rollback is the ClickHouse owner's: it drops fresh and creates gone
// again, leaves retained alone, and states what it cannot undo. The captured
// state the rollback starts from holds fresh and retained; the forward diff's
// capture is not changed.
func TestGeneratorUsesOwnerReversalAndReportsRecoveryLimits(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	after := declaration(c, "fresh")
	after.FeatureCoverage, err = chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("", "orders", "retained"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "this source does not declare the retained policy"}},
	})
	c.Assert(err, qt.IsNil)
	opts := options(c, runtime, platform.ClickHouse, declaration(c, "gone", "retained"), after)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 2)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	reverse := plan.Reverse.Diff.TablesModified[0]
	c.Assert(reverse.FeatureChanges, qt.HasLen, 2)
	c.Assert(reverse.Current.OwnedObjects.Refs(), qt.DeepEquals, []objectidentity.ID{
		chschema.RowPolicyRef("", "orders", "fresh"), chschema.RowPolicyRef("", "orders", "retained"),
	})
	sql, err := builtin.RenderSQLWithCapabilities(platform.ClickHouse, opts.Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "Recovery limit:")
	c.Assert(sql, qt.Contains, "cannot undo rows they read while the forward plan applied")
	c.Assert(sql, qt.Contains, "DROP ROW POLICY `fresh` ON `orders`;")
	c.Assert(sql, qt.Contains, "CREATE ROW POLICY `gone` ON `orders` USING (tenant = 1) AS PERMISSIVE TO ALL;")
	c.Assert(sql, qt.Not(qt.Contains), "retained")
	c.Assert(plan.PriorSchema.FeatureObjects.Equal(opts.DesiredSchema.FeatureObjects), qt.IsFalse)
	c.Assert(opts.Diff.TablesModified[0].Current.OwnedObjects.Refs(), qt.DeepEquals, []objectidentity.ID{
		chschema.RowPolicyRef("", "orders", "gone"), chschema.RowPolicyRef("", "orders", "retained"),
	})
}

// TestGeneratorProjectsImplicitStateFromTheOwner changes a restrictive
// ClickHouse row policy to one whose declaration leaves the composition to the
// server's default. The rollback starts from the policy as the owner predicts
// the forward plan leaves it, permissive with the new filter, and not from the
// declaration, which names no composition, or from the policy as it was.
func TestGeneratorProjectsImplicitStateFromTheOwner(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	declared := func(policy *chschema.DesiredRowPolicy) *schemamodel.Database {
		database := declaration(c)
		database.FeatureObjects, err = schemaext.NewObjects(schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "tenant"), Value: policy})
		c.Assert(err, qt.IsNil)
		return database
	}
	before := declared(&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true}})
	after := declared(&chschema.DesiredRowPolicy{Filter: new("tenant = 2"), Roles: chschema.RoleSelection{All: true}})
	opts := options(c, runtime, platform.ClickHouse, before, after)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	reverse := plan.Reverse.Diff.TablesModified[0]
	c.Assert(reverse.FeatureChanges, qt.HasLen, 1)
	change := reverse.FeatureChanges[0].Value.(*chdiff.RowPolicy)
	c.Assert(change.Before.Composition, qt.Equals, chschema.Permissive)
	object, found, err := reverse.Current.OwnedObjects.Get(reverse.FeatureChanges[0].Subject)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	projected := object.Value.(*chschema.ObservedRowPolicy)
	c.Assert(projected.Composition, qt.Equals, chschema.Permissive)
	c.Assert(*projected.Filter, qt.Equals, "tenant = 2")
}

func TestGeneratorProjectsOnlyAcceptedColumnChanges(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := declaration(c, "tenant")
	before.Fields = append(before.Fields, schemamodel.Field{StructName: "Order", Name: "retained", Type: "String", Nullable: true})
	after := declaration(c, "tenant")
	after.Fields = append(after.Fields, schemamodel.Field{StructName: "Order", Name: "added", Type: "String", Nullable: true})
	opts := options(c, runtime, platform.ClickHouse, before, after)
	filtered, skipped := diffpolicy.ApplyForDialect(opts.Diff, diffpolicy.NewSkipSet(planext.DropColumn), platform.ClickHouse)
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
	opts := options(c, selected, platform.ClickHouse, declaration(c), declaration(c, "tenant"))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(plan, qt.IsNil)
	c.Assert(selected.ctx, qt.Equals, t.Context())
	c.Assert(selected.request.Target, qt.Equals, platform.ClickHouse)
	c.Assert(selected.request.Changes, qt.HasLen, 1)
}

func TestGeneratorRejectsMissingRuntimeBeforePlanning(t *testing.T) {
	c := qt.New(t)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(plan, qt.IsNil)
}
