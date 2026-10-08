package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// unvalidatedSchema declares `p (id)` and `c (id, n, p_id)` with a CHECK and a
// foreign key the table may hold NOT VALID.
func unvalidatedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "n", Type: "int", Nullable: true},
			{StructName: "C", Name: "p_id", Type: "int", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "C", Table: "c", Name: "c_n_positive", Type: "CHECK", CheckExpression: "n > 0", NotValid: true},
			{
				StructName: "C", Table: "c", Name: "c_p_fkey", Type: "FOREIGN KEY", Columns: []string{"p_id"},
				ForeignTable: "p", ForeignColumn: "id", NotValid: true,
			},
		},
	}
}

// TestRender_AddsAnUnvalidatedConstraintAfterItsTable writes a CHECK and a
// foreign key the table may hold NOT VALID as additions after the table, where
// PostgreSQL 18.6 keeps the clause. Written inside the CREATE TABLE, the server
// records the CHECK validated, and the database built is not the one declared
// (stokaro/ptah#3853).
func TestRender_AddsAnUnvalidatedConstraintAfterItsTable(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(unvalidatedSchema(), "postgres",
		capability.Postgres18())

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `ALTER TABLE "c" ADD CONSTRAINT "c_n_positive" CHECK (n > 0) NOT VALID;`)
	c.Assert(sql, qt.Contains, `REFERENCES "p"("id") NOT VALID;`)
	c.Assert(strings.Count(sql, "c_n_positive"), qt.Equals, 1)
}

// TestRender_WritesAValidatedCheckInsideItsTable is the control: a CHECK
// without the clause stays in the CREATE TABLE.
func TestRender_WritesAValidatedCheckInsideItsTable(t *testing.T) {
	c := qt.New(t)
	schema := unvalidatedSchema()
	schema.Constraints[0].NotValid = false

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Not(qt.Contains), `ADD CONSTRAINT "c_n_positive"`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "c_n_positive" CHECK (n > 0)`)
}
