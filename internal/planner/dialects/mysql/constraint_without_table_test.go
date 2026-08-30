package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
			diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{test.constraint.Name}}
			desired := &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
				Constraints: []schemamodel.Constraint{test.constraint},
			}

			nodes, err := mysql.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))

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
	diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Booking", Name: "bookings"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "Booking", Name: "positive_price", Type: "CHECK",
			Table: "archived_bookings", CheckExpression: "price > 0",
		}},
	}

	nodes, err := mysql.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `archived_bookings` ADD CONSTRAINT `positive_price` CHECK (price > 0);")
}

// TestPlanner_ExcludeConstraintIsReportedRatherThanEmitted pins the third
// answer this pass can give.
//
// A named addition the diff does not describe is refused; a described CHECK the
// target does not enforce is reported (issue #226); and an EXCLUDE is described
// but has no spelling on any server this planner serves, so it is reported too.
// Answering the first for either of the others is the mistake, and this test
// and TestPlanner_CapabilityGating_CheckAddSkippedWhenUnenforced are what say
// so — the behavior was covered only by an integration test before, which
// `go test ./...` never compiles (stokaro/ptah#2315).
func TestPlanner_ExcludeConstraintIsReportedRatherThanEmitted(t *testing.T) {
	tests := []struct {
		name    string
		planner *mysql.Planner
		dialect string
		want    string
	}{
		{
			name:    "mysql",
			planner: mysql.New(),
			dialect: "mysql",
			want:    "WARNING: EXCLUDE constraint one_active_session_per_user not supported in MySQL (PostgreSQL-specific feature)",
		},
		{
			// The label names the server the author is pointed at. Oracle and
			// SQL Server borrow this planner, and being told MySQL cannot hold
			// the constraint is an answer about a server they are not running.
			name:    "oracle",
			planner: mysql.NewForDialect(platform.Oracle, capability.Oracle23()),
			dialect: "oracle",
			want:    "WARNING: EXCLUDE constraint one_active_session_per_user not supported in Oracle (PostgreSQL-specific feature)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"one_active_session_per_user"}}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "UserSession", Name: "user_sessions"}},
				Constraints: []schemamodel.Constraint{{
					StructName: "UserSession", Name: "one_active_session_per_user", Type: "EXCLUDE",
					Table: "user_sessions", UsingMethod: "gist", ExcludeElements: "user_id WITH =",
				}},
			}

			nodes, err := test.planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))

			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL(test.dialect, nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
