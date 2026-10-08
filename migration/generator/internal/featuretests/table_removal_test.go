package featuretests_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestTableRemovalCapturesUnknownOwnedStateBeforePolicy(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	opts := options(c, runtime, declaration(c, stream), declaration(c, stream))
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "items")
	opts.CurrentSchema.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Observed, []schemaext.SubjectCoverage{
		{Kind: ydbschema.ChangefeedKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "topic attributes were not inspected"}},
	})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, opts.CurrentSchema, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(diff.TablesRemoved[0].Current.OwnedObjects.Equal(opts.CurrentSchema.FeatureObjects), qt.IsTrue)
	c.Assert(diff.TablesRemoved[0].Current.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, parent).State, qt.Equals, schemaext.Uninspected)
	opts.CurrentSchema.Tables[0].Columns[0].Name = "mutated"
	c.Assert(diff.TablesRemoved[0].Current.Table.Columns[0].Name, qt.Equals, "id")
	kept, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex), "ydb")
	c.Assert(skipped, qt.HasLen, 0)
	c.Assert(kept.TablesRemoved[0].Current.OwnedObjects.Equal(diff.TablesRemoved[0].Current.OwnedObjects), qt.IsTrue)
	filtered, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(diffpolicy.DropTable), "ydb")
	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(filtered.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
}

func TestReverseTableRemovalProjectsAcceptedCreation(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	desired := declaration(c, stream)
	current := &catalog.Database{}
	caps := capability.YDB262()
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	desired.FeatureObjects = schemaext.Objects{}
	desired.Fields[0].Name = "not_accepted"
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
	removal := plan.Reverse.Diff.TablesRemoved[0]
	c.Assert(removal.Name, qt.Equals, "items")
	c.Assert(removal.Current.Table.Columns, qt.HasLen, 1)
	c.Assert(removal.Current.Table.Columns[0].Name, qt.Equals, "id")
	streams, err := ydbschema.ObservedChangefeeds(removal.Current.OwnedObjects, "", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(streams, qt.DeepEquals, []ydbschema.ChangefeedSpec{stream})
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "items")
	c.Assert(removal.Current.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, parent).State, qt.Equals, schemaext.Complete)
}

func TestReverseTableRemovalRetainsDeclaredEmptyNamespace(t *testing.T) {
	for _, caps := range []capability.Capabilities{capability.YDB251(), capability.YDB262()} {
		c := qt.New(t)
		runtime, err := builtin.New()
		c.Assert(err, qt.IsNil)
		desired := declaration(c)
		current := &catalog.Database{}
		current.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Observed, nil)
		c.Assert(err, qt.IsNil)
		diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
		c.Assert(err, qt.IsNil)
		// Subsequent edits to the document cannot erase the accepted knowledge.
		desired.FeatureCoverage = schemaext.Coverage{}
		plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
			Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps,
		})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
		removed := plan.Reverse.Diff.TablesRemoved[0].Current
		c.Assert(removed.OwnedObjects.Len(), qt.Equals, 0)
		parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("items")
		c.Assert(removed.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, parent).State, qt.Equals, schemaext.Complete)
	}
}

func TestReverseTableRemovalRefusesUndeclaredNamespace(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := declaration(c)
	desired.FeatureCoverage = schemaext.Coverage{}
	current := &catalog.Database{}
	current.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Observed, nil)
	c.Assert(err, qt.IsNil)
	caps := capability.YDB262()
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	// Adding a claim after comparison cannot strengthen the accepted creation.
	desired.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps,
	})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*changefeed namespace is not fully described.*`)
	c.Assert(plan, qt.IsNil)
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

func TestReverseTableCreationRestoresTheCapturedRemoval(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	opts := options(c, runtime, declaration(c, stream), &schemamodel.Database{})
	c.Assert(opts.Diff.TablesRemoved, qt.HasLen, 1)
	// The caller's catalog cannot replace the already captured removal.
	opts.CurrentSchema = &catalog.Database{}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesAdded, qt.HasLen, 1)
	restored := plan.Reverse.Diff.TablesAdded[0]
	c.Assert(restored.Table.Name, qt.Equals, "items")
	c.Assert(restored.Fields, qt.HasLen, 1)
	c.Assert(restored.Fields[0].Name, qt.Equals, "id")
	streams, err := ydbschema.DesiredChangefeeds(restored.OwnedObjects, "", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(streams, qt.DeepEquals, []ydbschema.ChangefeedSpec{stream})
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

func TestTableRestorationDoesNotDescribeUncapturedNamespaces(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	before := declaration(c, stream)
	kept := schemamodel.Table{Name: "kept", StructName: "Kept"}
	field := schemamodel.Field{Name: "id", StructName: "Kept", Type: "uint64", Primary: true}
	before.Tables = append(before.Tables, kept)
	before.Fields = append(before.Fields, field)
	opts := options(c, runtime, before, &schemamodel.Database{Tables: []schemamodel.Table{kept}, Fields: []schemamodel.Field{field}})
	c.Assert(opts.Diff.TablesRemoved.Names(), qt.DeepEquals, []string{"items"})
	opts.CurrentSchema.FeatureCoverage = schemaext.Coverage{}
	opts.CurrentSchema.FeatureObjects = schemaext.Objects{}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	c.Assert(plan.CurrentSchema.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, builder.Table("items")).State, qt.Equals, schemaext.Complete)
	c.Assert(plan.CurrentSchema.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, builder.Table("kept")).State, qt.Equals, schemaext.Uninspected)
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
