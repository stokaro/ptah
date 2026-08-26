package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// primaryKeyAdditionSQL plans one PRIMARY KEY addition and returns the SQL.
func primaryKeyAdditionSQL(c *qt.C, include []string) string {
	c.Helper()
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"covering_pkey"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name:           "covering_pkey",
			TableName:      "covering",
			Type:           "PRIMARY KEY",
			Columns:        []string{"a", "b"},
			IncludeColumns: include,
		}},
	}
	nodes, err := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return legacyRenderedSQL(sql)
}

// TestPlanner_AddsTheIncludePayloadOfACoveringPrimaryKey pins that the payload
// reaches the statement.
//
// A modified primary key is planned as a DROP and an ADD. The ADD built the
// constraint node from the column list alone, so the payload the DROP had just
// removed never came back and the live index was left plain -- the covering
// UNIQUE two cases above in the same switch already carried it
// (stokaro/ptah#2199).
func TestPlanner_AddsTheIncludePayloadOfACoveringPrimaryKey(t *testing.T) {
	c := qt.New(t)

	sql := primaryKeyAdditionSQL(c, []string{"payload"})

	c.Assert(sql, qt.Contains, `ADD PRIMARY KEY (a, b) INCLUDE (payload)`)
}

// TestPlanner_LeavesAPlainPrimaryKeyWithoutAnIncludeClause is the control.
//
// A key with no payload must not gain an empty INCLUDE, which PostgreSQL
// refuses outright.
func TestPlanner_LeavesAPlainPrimaryKeyWithoutAnIncludeClause(t *testing.T) {
	c := qt.New(t)

	sql := primaryKeyAdditionSQL(c, nil)

	c.Assert(sql, qt.Contains, `ADD PRIMARY KEY (a, b)`)
	c.Assert(sql, qt.Not(qt.Contains), "INCLUDE")
}

// declaredPrimaryKeyAdditionSQL plans a PRIMARY KEY that arrives by NAME only,
// resolved from the description's own constraint list.
//
// This is a second route to the same statement: additions carrying their table
// go through addPrimaryKeyConstraintsWithTables, and a constraint known only by
// name falls back to addConstraintNodeFor. Both build a constraint node, and
// only one of them was reached by the case above -- measured, the fallback
// really is reachable for a primary key, so leaving it unmeasured would leave
// half the fix unguarded.
func declaredPrimaryKeyAdditionSQL(c *qt.C, include []string) string {
	c.Helper()
	diff := &difftypes.SchemaDiff{ConstraintsAdded: []string{"covering_pkey"}}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Covering", Name: "covering"}},
		Constraints: []goschema.Constraint{{
			StructName:     "Covering",
			Name:           "covering_pkey",
			Type:           "PRIMARY KEY",
			Table:          "covering",
			Columns:        []string{"a", "b"},
			IncludeColumns: include,
		}},
	}
	nodes, err := postgres.New().GenerateMigrationAST(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return legacyRenderedSQL(sql)
}

// TestPlanner_AddsTheIncludePayloadOfAPrimaryKeyKnownByName covers the fallback
// route, which the addition-with-table case above never reaches.
func TestPlanner_AddsTheIncludePayloadOfAPrimaryKeyKnownByName(t *testing.T) {
	c := qt.New(t)

	sql := declaredPrimaryKeyAdditionSQL(c, []string{"payload"})

	c.Assert(sql, qt.Contains, `ADD PRIMARY KEY (a, b) INCLUDE (payload)`)
}

// TestPlanner_LeavesANamedPlainPrimaryKeyWithoutAnIncludeClause is that route's
// control.
func TestPlanner_LeavesANamedPlainPrimaryKeyWithoutAnIncludeClause(t *testing.T) {
	c := qt.New(t)

	sql := declaredPrimaryKeyAdditionSQL(c, nil)

	c.Assert(sql, qt.Contains, `ADD PRIMARY KEY (a, b)`)
	c.Assert(sql, qt.Not(qt.Contains), "INCLUDE")
}
