package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPlanner_TableLevelConstraintWithoutAnExplicitTable is issue #2008 on the
// other planner that renders a table-level constraint from a declaration.
//
// The declaration leaves `Table` empty whenever it matches the struct's table,
// which is the ordinary case, and the empty value reached the renderer as
// `ALTER TABLE ""`. EXCLUDE is PostgreSQL's alone and is refused here with a
// warning instead, so the kinds that reach the node are CHECK and UNIQUE.
func TestPlanner_TableLevelConstraintWithoutAnExplicitTable(t *testing.T) {
	tests := []struct {
		name       string
		constraint goschema.Constraint
		wantSQL    string
	}{
		{
			name: "CHECK",
			constraint: goschema.Constraint{
				StructName: "Booking", Name: "positive_price", Type: "CHECK",
				CheckExpression: "price > 0",
			},
			wantSQL: "ALTER TABLE `bookings` ADD CONSTRAINT `positive_price` CHECK (price > 0);",
		},
		{
			name: "UNIQUE",
			constraint: goschema.Constraint{
				StructName: "Booking", Name: "uq_booking_code", Type: "UNIQUE",
				Columns: []string{"code"},
			},
			wantSQL: "ALTER TABLE `bookings` ADD CONSTRAINT `uq_booking_code` UNIQUE (`code`);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{ConstraintsAdded: []string{test.constraint.Name}}
			generated := &goschema.Database{
				Tables:      []goschema.Table{{StructName: "Booking", Name: "bookings"}},
				Constraints: []goschema.Constraint{test.constraint},
			}

			nodes, err := mysql.New().GenerateMigrationASTChecked(diff, generated)

			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("mysql", nodes...)
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
	diff := &types.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Booking", Name: "bookings"}},
		Constraints: []goschema.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			Table: "archived_bookings", CheckExpression: "price > 0",
		}},
	}

	nodes, err := mysql.New().GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `archived_bookings` ADD CONSTRAINT `positive_price` CHECK (price > 0);")
}
