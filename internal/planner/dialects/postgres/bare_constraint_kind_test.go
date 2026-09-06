package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlannerRefusesABareNameConstraintAddition is what the bare-name ordering
// control became, and the change is the point.
//
// It used to assert that a constraint addition carrying nothing but a NAME was
// still classified by kind, so that a foreign key was added after the other
// kinds -- a FOREIGN KEY may reference columns a UNIQUE constraint in the same
// plan is what makes referenceable. The classification worked by resolving the
// name against the declaration handed to the planner.
//
// That resolution is withdrawn. A comparison describes every constraint it
// adds, the ones synthesized from a field's `check=` and `foreign=` included, so
// a name arriving without a definition is a caller error and is answered as one
// (stokaro/ptah#2315).
//
// The ordering property it protected did not go anywhere: it is asserted on
// records, which carry the kind directly, by
// TestPlannerOrdersAForeignKeyAfterTheOtherKinds below.
func TestPlannerRefusesABareNameConstraintAddition(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "aaa_fk_child_parent"}, {Name: "zzz_ck_child_amount"}},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)

	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*constraint "aaa_fk_child_parent" is added without a definition.*`)
}

// TestPlannerOrdersAForeignKeyAfterTheOtherKinds keeps the property the test
// above used to carry, on the input shape that remains.
//
// The names are chosen so alphabetical order is the WRONG answer -- `aaa_` for
// the foreign key, `zzz_` for the check -- because an ordering that collapsed to
// "emit in name order" would otherwise agree with the correct one by accident.
func TestPlannerOrdersAForeignKeyAfterTheOtherKinds(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
			{
				Name: "aaa_fk_child_parent", TableName: "wf2315_children", Type: "FOREIGN KEY",
				Columns: []string{"parent_id"}, ForeignTable: "wf2315_parents", ForeignColumn: "id",
				ForeignColumns: []string{"id"},
			},
			{
				Name: "zzz_ck_child_amount", TableName: "wf2315_children", Type: "CHECK",
				CheckExpression: "amount > 0",
			},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql := renderPostgresNodes(c, nodes)

	check := indexOfSubstring(sql, "zzz_ck_child_amount")
	foreignKey := indexOfSubstring(sql, "aaa_fk_child_parent")
	c.Assert(check, qt.Not(qt.Equals), -1, qt.Commentf("plan:\n%s", sql))
	c.Assert(foreignKey, qt.Not(qt.Equals), -1, qt.Commentf("plan:\n%s", sql))
	c.Assert(check < foreignKey, qt.IsTrue,
		qt.Commentf("a FOREIGN KEY must be added after the other kinds; plan:\n%s", sql))
}

// renderPostgresNodes renders a plan, failing the test rather than returning an
// error the caller would have to check.
func renderPostgresNodes(c *qt.C, nodes []ast.Node) string {
	c.Helper()
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// indexOfSubstring is strings.Index, named for what the assertions above read
// it as.
func indexOfSubstring(haystack, needle string) int {
	return strings.Index(haystack, needle)
}
