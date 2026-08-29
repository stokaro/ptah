package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff"
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
			desired := &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
				Fields:      []schemamodel.Field{{StructName: "Booking", Name: "code", Type: "TEXT"}},
				Constraints: []schemamodel.Constraint{test.constraint},
			}
			// A real comparison, because resolving the host table is what this
			// test is about and that resolution is the comparison's: the planner
			// renders the record, and the record carries the table the
			// declaration resolved to (stokaro/ptah#2315).
			diff := schemadiff.CompareWithDialect(desired, bookingsDatabase(), platform.Postgres)

			nodes, err := postgres.New().GenerateMigrationAST(diff, desired)

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
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Booking", Name: "bookings"},
			{StructName: "Archived", Name: "archived_bookings"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Booking", Name: "code", Type: "TEXT"},
			{StructName: "Archived", Name: "price", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			Table: "archived_bookings", CheckExpression: "price > 0",
		}},
	}
	diff := schemadiff.CompareWithDialect(desired, bookingsDatabase(), platform.Postgres)

	nodes, err := postgres.New().GenerateMigrationAST(diff, desired)

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "archived_bookings" ADD CONSTRAINT "positive_price" CHECK (price > 0);`)
}

// TestPlanner_RefusesAConstraintTheDiffDoesNotDescribe is what the last-resort
// test became, and the change is the one its own comment asked for.
//
// It read: "this planner has no way to refuse, and its two silent alternatives
// are worse than a wrong name" -- so a constraint whose struct no table declares
// was rendered against the STRUCT's name, `ALTER TABLE "Booking"`, chosen to fail
// loudly at the server rather than quietly in the plan.
//
// The planner has a way to refuse now. A diff that names a constraint it does
// not describe is a caller error, and saying so beats every spelling of a table
// nobody declared (stokaro/ptah#2315).
func TestPlanner_RefusesAConstraintTheDiffDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}

	nodes, err := postgres.New().GenerateMigrationAST(diff, &schemamodel.Database{})

	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*constraint "positive_price" is added without a definition.*`)
}

// bookingsDatabase is a database holding the tables the rows above declare and
// none of their constraints, so every constraint is an addition.
func bookingsDatabase() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{
		{
			Name: "bookings", Type: "BASE TABLE",
			Columns: []catalog.Column{{Name: "code", DataType: "text", IsNullable: "YES", OrdinalPosition: 1}},
		},
		{
			Name: "archived_bookings", Type: "BASE TABLE",
			Columns: []catalog.Column{{Name: "price", DataType: "integer", IsNullable: "YES", OrdinalPosition: 1}},
		},
	}}
}
