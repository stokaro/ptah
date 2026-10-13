package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_TableLevelConstraintWithoutAnExplicitTable is issue #2008 on the
// other planner that renders a table-level constraint from a declaration.
//
// The declaration leaves `Table` empty whenever it matches the struct's table,
// which is the ordinary case, and the empty value reached the renderer as
// `ALTER TABLE ""`. These rows cover CHECK and UNIQUE. Feature-owned constraints
// are validated by the selected runtime.
func TestPlanner_TableLevelConstraintWithoutAnExplicitTable(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		wantSQL    string
	}{
		{
			name: "CHECK",
			constraint: schemamodel.Constraint{
				StructName: "Booking", Name: "positive_price", Type: "CHECK",
				CheckExpression: "price > 0",
			},
			wantSQL: "ALTER TABLE `bookings` ADD CONSTRAINT `positive_price` CHECK (price > 0);",
		},
		{
			name: "UNIQUE",
			constraint: schemamodel.Constraint{
				StructName: "Booking", Name: "uq_booking_code", Type: "UNIQUE",
				Columns: []string{"code"},
			},
			wantSQL: "ALTER TABLE `bookings` ADD CONSTRAINT `uq_booking_code` UNIQUE (`code`);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
				Constraints: []schemamodel.Constraint{test.constraint},
			}
			diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditionsFor(desired, test.constraint.Name)}

			nodes, err := mysql.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				withDeclaredObjects(diff, desired),
			)

			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantSQL)
		})
	}
}

// TestPlanner_TableLevelConstraintNamesItsOwnTable is the control: a
// declaration that names a table keeps naming it, and the struct's own table is
// something else so the two answers cannot be confused.
func TestPlanner_TableLevelConstraintNamesItsOwnTable(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			Table: "archived_bookings", CheckExpression: "price > 0",
		}},
	}
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditionsFor(desired, "positive_price")}

	nodes, err := mysql.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired),
	)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `archived_bookings` ADD CONSTRAINT `positive_price` CHECK (price > 0);")
}
