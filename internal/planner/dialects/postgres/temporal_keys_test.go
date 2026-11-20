package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// TestPlanner_AnAddedConstraintKeepsItsTemporalClause adds a UNIQUE, a primary
// key and a foreign key the comparison reports with the temporal clause the
// addition carries. A key added without it accepts the overlapping rows the
// declaration forbids (stokaro/ptah#6330).
func TestPlanner_AnAddedConstraintKeepsItsTemporalClause(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{
		{
			Name: "t_uq", TableName: "t", Type: "UNIQUE", Columns: []string{"id", "valid_at"}, WithoutOverlaps: true,
		},
		{
			Name: "t_pkey", TableName: "t", Type: "PRIMARY KEY", Columns: []string{"id", "valid_at"}, WithoutOverlaps: true,
		},
		{
			Name: "t_p_fkey", TableName: "t", Type: "FOREIGN KEY", Columns: []string{"pid", "valid_at"},
			ForeignTable: "parents", ForeignColumn: "id", ForeignColumns: []string{"id", "valid_at"}, Period: true,
		},
	}}

	sql := planPostgres18(c, diff, &schemamodel.Database{})

	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "t_uq" UNIQUE ("id", "valid_at" WITHOUT OVERLAPS);`)
	c.Assert(sql, qt.Contains, `ADD PRIMARY KEY ("id", "valid_at" WITHOUT OVERLAPS);`)
	c.Assert(sql, qt.Contains,
		`ADD CONSTRAINT "t_p_fkey" FOREIGN KEY ("pid", PERIOD "valid_at") REFERENCES "parents"("id", PERIOD "valid_at");`)
}

// TestPlanner_AnAddedConstraintWithoutTheFlagsStaysPlain is the control for the
// test above.
func TestPlanner_AnAddedConstraintWithoutTheFlagsStaysPlain(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{
		{Name: "t_uq", TableName: "t", Type: "UNIQUE", Columns: []string{"id", "valid_at"}},
		{
			Name: "t_p_fkey", TableName: "t", Type: "FOREIGN KEY", Columns: []string{"pid", "valid_at"},
			ForeignTable: "parents", ForeignColumn: "id", ForeignColumns: []string{"id", "valid_at"},
		},
	}}

	sql := planPostgres18(c, diff, &schemamodel.Database{})

	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "t_uq" UNIQUE ("id", "valid_at");`)
	c.Assert(sql, qt.Contains, `FOREIGN KEY ("pid", "valid_at") REFERENCES "parents"("id", "valid_at");`)
}
