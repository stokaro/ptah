package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// unvalidatedCheckDatabase is a table with a CHECK it holds NOT VALID or not.
func unvalidatedCheckDatabase(notValid bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "pos", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_pos_positive", Type: "CHECK",
			CheckExpression: "pos > 0", NotValid: notValid,
		}},
	}
}

// TestRender_NotValidConstraint_FailurePath refuses a constraint the server
// has not validated. Written without the clause, it reads back validated, and
// a plan from the export to the database it came from would validate it
// (stokaro/ptah#3853).
func TestRender_NotValidConstraint_FailurePath(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(unvalidatedCheckDatabase(true), "postgres", "public")

	c.Assert(err, qt.ErrorMatches, `CHECK "slots_pos_positive" is NOT VALID, which Atlas HCL cannot represent`)
	c.Assert(result, qt.DeepEquals, atlashclrender.Result{})
}

// TestRender_NotValidConstraint_HappyPath is the control: the validated CHECK
// is written.
func TestRender_NotValidConstraint_HappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(unvalidatedCheckDatabase(false), "postgres", "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `slots_pos_positive`)
}
