package featuretests_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// ordersTable is the identity of table orders on ClickHouse, the parent of
// every row policy [declaration] holds.
func ordersTable() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse)).TableParts("", "orders")
}

// tenantPolicy is the observation of the row policy [declaration] declares.
func tenantPolicy() *chschema.ObservedRowPolicy {
	return &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true}}
}

// TestTableRemovalCapturesUnknownOwnedStateBeforePolicy compares an empty
// declaration with a ClickHouse read of orders, which holds a row policy and
// whose policies the read did not fully inspect. The removal captures the
// policy and the unknown knowledge, independently of the catalog it was read
// from. A diff policy that skips something else keeps the removal with its
// capture; one that skips the drop removes it from a copy of the diff only.
func TestTableRemovalCapturesUnknownOwnedStateBeforePolicy(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	opts := options(c, runtime, platform.ClickHouse, declaration(c, "tenant"), declaration(c, "tenant"))
	parent := ordersTable()
	opts.CurrentSchema.FeatureCoverage, err = chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: chschema.RowPolicyKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the roles of row policies were not inspected"}},
	})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, opts.CurrentSchema, platform.ClickHouse, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(diff.TablesRemoved[0].Current.OwnedObjects.Equal(opts.CurrentSchema.FeatureObjects), qt.IsTrue)
	c.Assert(diff.TablesRemoved[0].Current.OwnedObjects.Len(), qt.Equals, 1)
	c.Assert(diff.TablesRemoved[0].Current.FeatureCoverage.Lookup(chschema.RowPolicyKind, parent).State, qt.Equals, schemaext.Uninspected)
	opts.CurrentSchema.Tables[0].Columns[0].Name = "mutated"
	c.Assert(diff.TablesRemoved[0].Current.Table.Columns[0].Name, qt.Equals, "id")
	kept, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(planext.DropIndex), platform.ClickHouse)
	c.Assert(skipped, qt.HasLen, 0)
	c.Assert(kept.TablesRemoved, qt.HasLen, 1)
	c.Assert(kept.TablesRemoved[0].Current.OwnedObjects.Equal(diff.TablesRemoved[0].Current.OwnedObjects), qt.IsTrue)
	c.Assert(kept.TablesRemoved[0].Current.FeatureCoverage.Lookup(chschema.RowPolicyKind, parent).State, qt.Equals, schemaext.Uninspected)
	filtered, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(planext.DropTable), platform.ClickHouse)
	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(filtered.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
}

// TestReverseTableRemovalProjectsAcceptedCreation plans the creation of
// ClickHouse table orders with a row policy, then edits the document. The
// rollback drops the table the comparison accepted: its capture holds the
// accepted column and the policy as an observation, with complete knowledge
// of the table's policies, and the rollback drops the policy, which ClickHouse
// keeps when the table goes.
func TestReverseTableRemovalProjectsAcceptedCreation(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := declaration(c, "tenant")
	current := &catalog.Database{}
	caps := capability.ForDialect(platform.ClickHouse)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.ClickHouse, Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	desired.FeatureObjects = schemaext.Objects{}
	desired.Fields[0].Name = "not_accepted"
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.ClickHouse, Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
	removal := plan.Reverse.Diff.TablesRemoved[0]
	c.Assert(removal.Name, qt.Equals, "orders")
	c.Assert(removal.Current.Table.Columns, qt.HasLen, 1)
	c.Assert(removal.Current.Table.Columns[0].Name, qt.Equals, "id")
	objects, err := removal.Current.OwnedObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, chschema.RowPolicyRef("", "orders", "tenant"))
	c.Assert(objects[0].Value.Equal(tenantPolicy()), qt.IsTrue)
	c.Assert(removal.Current.FeatureCoverage.Lookup(chschema.RowPolicyKind, ordersTable()).State, qt.Equals, schemaext.Complete)
	sql, err := builtin.RenderSQLWithCapabilities(platform.ClickHouse, caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP ROW POLICY")
}

// TestReverseTableRemovalRetainsDeclaredEmptyNamespace plans the creation of
// a ClickHouse table whose declaration holds no row policy and claims to
// describe them all, then drops that claim from the document. The rollback's
// capture of the table keeps the claim the comparison accepted: the table has
// no policy, and that is known.
func TestReverseTableRemovalRetainsDeclaredEmptyNamespace(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := declaration(c)
	current := &catalog.Database{}
	current.FeatureCoverage, err = chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(platform.ClickHouse)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.ClickHouse, Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	desired.FeatureCoverage = schemaext.Coverage{}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.ClickHouse, Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
	removed := plan.Reverse.Diff.TablesRemoved[0].Current
	c.Assert(removed.OwnedObjects.Len(), qt.Equals, 0)
	c.Assert(removed.FeatureCoverage.Lookup(chschema.RowPolicyKind, ordersTable()).State, qt.Equals, schemaext.Complete)
}

// TestReverseTableRemovalDoesNotStrengthenAnUndeclaredNamespace plans the
// creation of a ClickHouse table from a declaration that makes no claim about
// row policies, then adds a complete claim to the document. The rollback's
// capture keeps what the comparison accepted, which leaves the table's
// policies unknown, rather than the claim the document gained afterwards.
func TestReverseTableRemovalDoesNotStrengthenAnUndeclaredNamespace(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := declaration(c)
	desired.FeatureCoverage = schemaext.Coverage{}
	current := &catalog.Database{}
	current.FeatureCoverage, err = chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(platform.ClickHouse)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.ClickHouse, Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	desired.FeatureCoverage, err = chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.ClickHouse, Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
	removed := plan.Reverse.Diff.TablesRemoved[0].Current
	c.Assert(removed.OwnedObjects.Len(), qt.Equals, 0)
	c.Assert(removed.FeatureCoverage.Lookup(chschema.RowPolicyKind, ordersTable()).State, qt.Equals, schemaext.Uninspected)
}

func TestReverseTableRemovalCapturesAcceptedCommonChildren(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:      []schemamodel.Field{{StructName: "Item", Name: "id", Type: "int", Primary: true}, {StructName: "Item", Name: "quantity", Type: "int"}},
		Indexes:     []schemamodel.Index{{StructName: "Item", Name: "quantity_index", Fields: []string{"quantity"}}},
		Constraints: []schemamodel.Constraint{{StructName: "Item", Name: "positive_quantity", Type: "CHECK", CheckExpression: "quantity > 0"}},
		Triggers:    []schemamodel.Trigger{{Name: "items_trigger", Table: "items", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "BEGIN RETURN NEW; END;"}},
	}
	current := &catalog.Database{}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	desired.Indexes = nil
	desired.Constraints = nil
	desired.Triggers = nil
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
	observed := plan.Reverse.Diff.TablesRemoved[0].Current
	c.Assert(slices.ContainsFunc(observed.Indexes, func(index catalog.Index) bool {
		return index.Name == "quantity_index" && slices.Equal(index.Columns, []string{"quantity"})
	}), qt.IsTrue)
	c.Assert(slices.ContainsFunc(observed.Constraints, func(constraint catalog.Constraint) bool {
		return constraint.Name == "positive_quantity" && constraint.CheckClause != nil && *constraint.CheckClause == "quantity > 0"
	}), qt.IsTrue)
	c.Assert(observed.Triggers, qt.HasLen, 1)
	c.Assert(observed.Triggers[0].Name, qt.Equals, "items_trigger")
}

func TestReverseTableRemovalOrderUsesCapturedDependencies(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "accounts", StructName: "Account"}, {Name: "projects", StructName: "Project"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "int", Primary: true},
			{StructName: "Project", Name: "id", Type: "int", Primary: true},
			{StructName: "Project", Name: "account_id", Type: "int", Foreign: "accounts(id)"},
		},
	}
	current := &catalog.Database{}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: &schemamodel.Database{}, CurrentSchema: current, Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved.Names(), qt.DeepEquals, []string{"projects", "accounts"})
}

// TestReverseTableCreationRestoresTheCapturedRemoval plans the removal of
// ClickHouse table orders, which holds a row policy, and then hands the planner
// an empty catalog in place of the one compared. The rollback creates the
// table and its policy from the removal the comparison captured, and changes
// neither the catalog nor the forward diff.
func TestReverseTableCreationRestoresTheCapturedRemoval(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	opts := options(c, runtime, platform.ClickHouse, declaration(c, "tenant"), &schemamodel.Database{})
	c.Assert(opts.Diff.TablesRemoved, qt.HasLen, 1)
	opts.CurrentSchema = &catalog.Database{}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesAdded, qt.HasLen, 1)
	restored := plan.Reverse.Diff.TablesAdded[0]
	c.Assert(restored.Table.Name, qt.Equals, "orders")
	c.Assert(restored.Fields, qt.HasLen, 1)
	c.Assert(restored.Fields[0].Name, qt.Equals, "id")
	objects, err := restored.OwnedObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, chschema.RowPolicyRef("", "orders", "tenant"))
	sql, err := builtin.RenderSQLWithCapabilities(platform.ClickHouse, opts.Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE ROW POLICY")
	c.Assert(opts.CurrentSchema.Tables, qt.HasLen, 0)
	c.Assert(opts.Diff.TablesRemoved[0].Current.OwnedObjects.Len(), qt.Equals, 1)
}

func TestReverseTableCreationRestoresCapturedCommonChildren(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:      []schemamodel.Field{{StructName: "Item", Name: "id", Type: "int", Primary: true}},
		Indexes:     []schemamodel.Index{{StructName: "Item", Name: "items_index", Fields: []string{"id"}}},
		Constraints: []schemamodel.Constraint{{StructName: "Item", Name: "positive_id", Type: "CHECK", CheckExpression: "id > 0"}},
		Triggers:    []schemamodel.Trigger{{Name: "items_trigger", Table: "items", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "BEGIN RETURN NEW; END;"}},
	}
	current, err := goschematodb.ToDBSchema(t.Context(), before, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: &catalog.Database{}, Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesAdded, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.TablesAdded[0].Fields[0].Name, qt.Equals, "id")
	c.Assert(plan.Reverse.Diff.IndexesAdded, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.IndexesAdded[0].Index.Fields, qt.DeepEquals, []string{"id"})
	c.Assert(slices.ContainsFunc(plan.Reverse.Diff.ConstraintsAdded, func(constraint difftypes.ConstraintAdditionInfo) bool {
		return constraint.Name == "positive_id" && constraint.CheckExpression == "id > 0"
	}), qt.IsTrue)
	c.Assert(plan.Reverse.Diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.TriggersAdded[0].Desired.Body, qt.Equals, "BEGIN RETURN NEW; END;")
}

// TestTableRestorationDoesNotDescribeUncapturedNamespaces plans the removal
// of ClickHouse table orders beside a kept table, from a catalog whose row
// policy knowledge is then cleared. The rollback source describes the policies
// of the restored table as its removal captured them, and leaves the kept
// table's policies unknown: knowledge captured for one table does not describe
// another. The caller's catalog is not changed.
func TestTableRestorationDoesNotDescribeUncapturedNamespaces(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := declaration(c, "tenant")
	kept := schemamodel.Table{Name: "kept", StructName: "Kept", Engine: "MergeTree"}
	field := schemamodel.Field{Name: "id", StructName: "Kept", Type: "UInt64", Primary: true}
	before.Tables = append(before.Tables, kept)
	before.Fields = append(before.Fields, field)
	opts := options(c, runtime, platform.ClickHouse, before, &schemamodel.Database{Tables: []schemamodel.Table{kept}, Fields: []schemamodel.Field{field}})
	c.Assert(opts.Diff.TablesRemoved.Names(), qt.DeepEquals, []string{"orders"})
	opts.CurrentSchema.FeatureCoverage = schemaext.Coverage{}
	opts.CurrentSchema.FeatureObjects = schemaext.Objects{}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse))
	c.Assert(plan.CurrentSchema.FeatureCoverage.Lookup(chschema.RowPolicyKind, builder.Table("orders")).State, qt.Equals, schemaext.Complete)
	c.Assert(plan.CurrentSchema.FeatureCoverage.Lookup(chschema.RowPolicyKind, builder.Table("kept")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(opts.CurrentSchema.FeatureCoverage.IsZero(), qt.IsTrue)
}

func TestTableRestorationRefusesInvalidCaptures(t *testing.T) {
	for _, test := range []struct {
		name     string
		removals difftypes.TableRemovals
	}{
		{name: "missing observation", removals: difftypes.TableRemovals{{Name: "items"}}},
		{name: "different table", removals: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "other"}}}}},
		{name: "duplicate removal", removals: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}}}, {Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}}}}},
		{name: "foreign child", removals: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}, Indexes: []catalog.Index{{Name: "other_index", TableName: "other"}}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: &difftypes.SchemaDiff{TablesRemoved: test.removals}, DesiredSchema: &schemamodel.Database{}, CurrentSchema: &catalog.Database{}, Dialect: "postgres",
			})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(plan, qt.IsNil)
		})
	}
}
