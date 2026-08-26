package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_TableLevelConstraintWithoutAnExplicitTable pins issue #2008.
//
// `schemamodel.Constraint.Table` is documented "if different from struct name", so
// the ordinary declaration leaves it empty -- and the empty value reached the
// renderer:
//
//	ALTER TABLE "" ADD CONSTRAINT "ex1" EXCLUDE USING gist (room WITH =);
//
// which no server takes. Every kind that reaches this path was affected, not
// only the EXCLUDE the issue was filed for, because they all render through the
// same node.
//
// Each row declares the struct's table, so the expectation is the resolved
// name rather than the struct's -- the last-resort fallback is a separate
// question from this one.
func TestPlanner_TableLevelConstraintWithoutAnExplicitTable(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		wantSQL    string
	}{
		{
			name: "EXCLUDE",
			constraint: schemamodel.Constraint{
				StructName: "Booking", Name: "no_overlap", Type: "EXCLUDE",
				UsingMethod: "gist", ExcludeElements: "room WITH =",
			},
			wantSQL: `ALTER TABLE "bookings" ADD CONSTRAINT "no_overlap" EXCLUDE USING gist (room WITH =);`,
		},
		{
			name: "CHECK",
			constraint: schemamodel.Constraint{
				StructName: "Booking", Name: "positive_price", Type: "CHECK",
				CheckExpression: "price > 0",
			},
			wantSQL: `ALTER TABLE "bookings" ADD CONSTRAINT "positive_price" CHECK (price > 0);`,
		},
		{
			name: "UNIQUE",
			constraint: schemamodel.Constraint{
				StructName: "Booking", Name: "uq_booking_code", Type: "UNIQUE",
				Columns: []string{"code"},
			},
			wantSQL: `ALTER TABLE "bookings" ADD CONSTRAINT "uq_booking_code" UNIQUE ("code");`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{test.constraint.Name}}
			desired := &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
				Constraints: []schemamodel.Constraint{test.constraint},
			}

			nodes, err := postgres.New().GenerateMigrationASTChecked(diff, desired)

			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantSQL)
		})
	}
}

// TestPlanner_TableLevelConstraintNamesItsOwnTable is the control: a
// declaration that DOES name a table keeps naming it, and the struct's table is
// deliberately something else so the two answers cannot be confused.
func TestPlanner_TableLevelConstraintNamesItsOwnTable(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			Table: "archived_bookings", CheckExpression: "price > 0",
		}},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, desired)

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "archived_bookings" ADD CONSTRAINT "positive_price" CHECK (price > 0);`)
}

// TestPlanner_TableLevelConstraintWhoseStructDeclaresNoTable pins the last
// resort, and the choice it encodes.
//
// A description whose constraint names a struct no table declares is malformed
// -- the canonical adapter refuses it by name -- but this planner has no way to
// refuse, and its two silent alternatives are worse than a wrong name: an empty
// one renders `ALTER TABLE ""`, and dropping the node emits nothing at all for a
// constraint the description declared. The struct's own name fails loudly and
// says which declaration to look at, which is the fallback the field-level
// paths beside it already use.
func TestPlanner_TableLevelConstraintWhoseStructDeclaresNoTable(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}
	desired := &schemamodel.Database{
		Constraints: []schemamodel.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			CheckExpression: "price > 0",
		}},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, desired)

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "Booking" ADD CONSTRAINT "positive_price" CHECK (price > 0);`)
}
