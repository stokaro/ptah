package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_CarriesEnforcementAndMatch describes a declaration the way a
// catalog would report it, enforcement and the MATCH type included, so a
// schema file compared with itself pairs every such constraint with itself
// (stokaro/ptah#3853).
func TestToDBSchema_CarriesEnforcementAndMatch(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "integer", Primary: true},
			{StructName: "C", Name: "id", Type: "integer", Primary: true},
			{
				StructName: "C", Name: "p_id", Type: "integer", Nullable: true, Foreign: "p(id)",
				ForeignKeyName: "c_p_fkey", ForeignKeyMatch: "FULL", ForeignKeyNotEnforced: true,
			},
			{StructName: "C", Name: "n", Type: "integer", Nullable: true, Check: "n > 0", CheckName: "c_n_check", CheckNotEnforced: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "C", Table: "c", Name: "c_id_positive", Type: "CHECK", CheckExpression: "id > 0", NotEnforced: true,
		}},
	}

	got := goschematodb.ToDBSchema(desired, "postgres")

	described := make(map[string]catalog.Constraint)
	for _, constraint := range got.Constraints {
		described[constraint.Name] = constraint
	}
	c.Assert(described["c_p_fkey"].Match, qt.Equals, "FULL")
	c.Assert(described["c_p_fkey"].NotEnforced, qt.IsTrue)
	c.Assert(described["c_n_check"].NotEnforced, qt.IsTrue)
	c.Assert(described["c_id_positive"].NotEnforced, qt.IsTrue)
}
