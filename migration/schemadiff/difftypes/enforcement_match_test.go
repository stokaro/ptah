package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// enforcementDeclaration declares `p (id)` and `c (id, p_id, n)` whose column
// foreign key has MATCH FULL and whose column CHECK the server does not check,
// beside a table foreign key with MATCH PARTIAL that the server does not check.
func enforcementDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "integer", Primary: true},
			{StructName: "C", Name: "id", Type: "integer", Primary: true},
			{StructName: "C", Name: "p_id", Type: "integer", Nullable: true, Foreign: "p(id)", ForeignKeyMatch: "FULL"},
			{StructName: "C", Name: "n", Type: "integer", Nullable: true, Check: "n > 0", CheckName: "c_n_check", CheckNotEnforced: true},
			{StructName: "C", Name: "q_id", Type: "integer", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "C", Table: "c", Name: "c_q_fkey", Type: "FOREIGN KEY", Columns: []string{"q_id"},
			ForeignTable: "p", ForeignColumn: "id", Match: "PARTIAL", NotEnforced: true,
		}},
	}
}

// TestConstraintAdditionsFor_CarriesEnforcementAndMatch hands the planner the
// clauses a declared constraint carries, and the enforcement of a column's
// CHECK, which the comparison folds in beside the table's (stokaro/ptah#3853).
func TestConstraintAdditionsFor_CarriesEnforcementAndMatch(t *testing.T) {
	c := qt.New(t)

	additions := difftypes.ConstraintAdditionsFor(enforcementDeclaration(), "c_q_fkey", "c_n_check")

	c.Assert(additions, qt.HasLen, 2)
	c.Assert(additions[0].Match, qt.Equals, "PARTIAL")
	c.Assert(additions[0].NotEnforced, qt.IsTrue)
	c.Assert(additions[1].NotEnforced, qt.IsTrue)
}

// TestForeignKeyDeclarationsOf_CarriesTheMatchType keeps a foreign key's MATCH
// type, on a column and on the table, for the key a plan puts back around a
// change to one of its columns.
func TestForeignKeyDeclarationsOf_CarriesTheMatchType(t *testing.T) {
	c := qt.New(t)

	declarations := difftypes.ForeignKeyDeclarationsOf(enforcementDeclaration())

	c.Assert(declarationMatches(declarations), qt.DeepEquals, map[string]string{"fk_c_p_id": "FULL", "c_q_fkey": "PARTIAL"})
}

func declarationMatches(declarations []difftypes.ForeignKeyDeclaration) map[string]string {
	matches := make(map[string]string, len(declarations))
	for _, declaration := range declarations {
		matches[declaration.Name] = declaration.Match
	}
	return matches
}
