package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/migration/safety"
)

// The safety report for a column made NOT NULL with no default is one
// statement: a warning that it fails on a NULL row, carrying the plan's
// comment that says why. No statement rewrites a row (stokaro/ptah#3648).
func TestAssessRendered_SetNotNullWithoutDefaultWarnsAndRewritesNothing(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{
		Name: "flags",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
			Column:     ast.NewColumn("qty", "INTEGER").SetNotNull(),
			Changed:    ast.ColumnProperties{Nullability: true},
			HasChanged: true,
		}},
	}

	assessments, err := safety.AssessRendered([]ast.Node{node}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Warning)
	c.Assert(assessments[0].Reason, qt.Equals, "SET NOT NULL can fail when existing rows contain NULL")
	c.Assert(assessments[0].Statement, qt.Equals, "-- ALTER statements: --\n"+
		"-- POSTGRES: SET NOT NULL fails if any row of \"flags\" holds NULL in \"qty\"; the column declares no default to fill it with.\n"+
		`ALTER TABLE "flags" ALTER COLUMN "qty" SET NOT NULL`)
}
