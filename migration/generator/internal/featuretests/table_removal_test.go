package featuretests_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

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
