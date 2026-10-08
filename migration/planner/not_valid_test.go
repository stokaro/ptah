package planner_test

import (
	"context"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// unvalidatedAdditions are a CHECK and a foreign key the declaration allows NOT
// VALID, added to tables that exist.
var unvalidatedAdditions = []struct {
	name       string
	constraint difftypes.ConstraintAdditionInfo
	want       string
}{
	{
		name: "check",
		constraint: difftypes.ConstraintAdditionInfo{
			Name: "ck_amount", TableName: "orders", Type: "CHECK", CheckExpression: "amount > 0", NotValid: true,
		},
		want: `ALTER TABLE "orders" ADD CONSTRAINT "ck_amount" CHECK (amount > 0) NOT VALID;`,
	},
	{
		name: "foreign key",
		constraint: difftypes.ConstraintAdditionInfo{
			Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY", Columns: []string{"customer_id"},
			ForeignTable: "customers", ForeignColumn: "id", NotValid: true,
		},
		want: `REFERENCES "customers"("id") NOT VALID;`,
	},
}

// TestPlan_AddsAConstraintTheDeclarationAllowsNotValid adds the constraint NOT
// VALID and does not validate it, with and without the online request: the
// author asked for the rows already in the table to stay unchecked
// (stokaro/ptah#3853).
func TestPlan_AddsAConstraintTheDeclarationAllowsNotValid(t *testing.T) {
	for _, online := range []bool{false, true} {
		for _, test := range unvalidatedAdditions {
			t.Run(test.name+"/online="+strconv.FormatBool(online), func(t *testing.T) {
				c := qt.New(t)

				sql, err := planner.GenerateSchemaDiffSQLWithOptions(
					context.Background(), must.Must(builtin.New()),
					&difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{test.constraint}},
					"postgres",
					planner.Options{Capabilities: capability.Postgres18(), OnlineAlter: online},
				)

				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Contains, test.want)
				c.Assert(sql, qt.Not(qt.Contains), "VALIDATE CONSTRAINT")
			})
		}
	}
}

// TestPlan_ValidatesAConstraintTheDatabaseHoldsNotValid completes a constraint
// the database holds NOT VALID and the declaration holds validated.
func TestPlan_ValidatesAConstraintTheDatabaseHoldsNotValid(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{ConstraintsValidated: []difftypes.ConstraintValidation{{TableName: "orders", Name: "ck_amount"}}},
		"postgres",
		planner.Options{Capabilities: capability.Postgres18()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "orders" VALIDATE CONSTRAINT "ck_amount";`)
}
