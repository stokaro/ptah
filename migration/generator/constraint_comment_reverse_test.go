package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// planConstraintCommentRollback plans diff in both directions against current
// on PostgreSQL 18 and renders the rollback.
func planConstraintCommentRollback(c *qt.C, diff *difftypes.SchemaDiff, current *catalog.Database) (*difftypes.SchemaDiff, string) {
	c.Helper()
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: current,
		Dialect:       platform.Postgres,
		Capabilities:  capability.Postgres18(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return plan.Reverse.Diff, sql
}

// A rollback puts back the comment a forward change replaced on a constraint
// both sides keep (stokaro/ptah#3678).
func TestPlanBidirectionalSchemaDiff_ConstraintCommentRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintCommentsChanged: []difftypes.ConstraintCommentChange{
		{TableName: "app.orders", Name: "orders_total_positive", Current: "old", Desired: "new"},
	}}

	reversed, sql := planConstraintCommentRollback(c, diff, &catalog.Database{})

	c.Assert(reversed.ConstraintCommentsChanged, qt.DeepEquals, []difftypes.ConstraintCommentChange{
		{TableName: "app.orders", Name: "orders_total_positive", Current: "new", Desired: "old"},
	})
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "orders_total_positive" ON "app"."orders" IS 'old';`)
	c.Assert(diff.ConstraintCommentsChanged[0].Desired, qt.Equals, "new",
		qt.Commentf("the reversal must not write through to the forward diff"))
}

// A rollback that adds back a constraint the forward change dropped adds its
// comment too, read from the database the forward change started from.
func TestPlanBidirectionalSchemaDiff_DroppedConstraintComesBackWithItsComment(t *testing.T) {
	tests := []struct {
		name       string
		constraint catalog.Constraint
		want       string
	}{
		{
			name: "a CHECK",
			constraint: catalog.Constraint{
				Name: "orders_total_positive", TableName: "orders", Schema: "app", Type: "CHECK",
				CheckClause: new("total > 0"), Comment: "a total is positive",
			},
			want: `COMMENT ON CONSTRAINT "orders_total_positive" ON "app"."orders" IS 'a total is positive';`,
		},
		{
			name: "a UNIQUE",
			constraint: catalog.Constraint{
				Name: "orders_code_uq", TableName: "orders", Schema: "app", Type: "UNIQUE",
				ColumnNames: []string{"code"}, ColumnName: "code", Comment: "one order per code",
			},
			want: `COMMENT ON CONSTRAINT "orders_code_uq" ON "app"."orders" IS 'one order per code';`,
		},
		{
			name: "a FOREIGN KEY",
			constraint: catalog.Constraint{
				Name: "orders_customer_fk", TableName: "orders", Schema: "app", Type: "FOREIGN KEY",
				ColumnNames: []string{"customer_id"}, ColumnName: "customer_id",
				ForeignTable: new("customers"), ForeignSchema: "app", ForeignColumns: []string{"id"},
				Comment: "an order has a customer",
			},
			want: `COMMENT ON CONSTRAINT "orders_customer_fk" ON "app"."orders" IS 'an order has a customer';`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{
				Name: test.constraint.Name, TableName: "app.orders", Type: test.constraint.Type,
			}}}

			_, sql := planConstraintCommentRollback(c, diff, &catalog.Database{Constraints: []catalog.Constraint{test.constraint}})

			c.Assert(sql, qt.Contains, test.want, qt.Commentf("rollback:\n%s", sql))
		})
	}
}
