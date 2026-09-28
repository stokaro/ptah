package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// clauseDatabase is `p (id)` and `c (id, p_id, n)` whose column CHECK and
// column foreign key carry what the arguments say, beside a table CHECK and a
// table foreign key that carry the same.
func clauseDatabase(checkNotEnforced, keyNotEnforced bool, match string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "C", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "C", Name: "p_id", Type: "INTEGER", Nullable: true, Foreign: "p(id)",
				ForeignKeyName: "c_p_fkey", ForeignKeyMatch: match, ForeignKeyNotEnforced: keyNotEnforced,
			},
			{StructName: "C", Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0", CheckNotEnforced: checkNotEnforced},
		},
	}
}

// TestRender_EnforcementAndMatch_FailurePath refuses a constraint the server
// does not check, and a foreign key's MATCH type: the check and foreign_key
// blocks have no attribute for either, and written without them the
// constraint checks what its author said it does not (stokaro/ptah#3853).
func TestRender_EnforcementAndMatch_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		wantErr  string
	}{
		{
			name:     "a CHECK not enforced",
			database: clauseDatabase(true, false, ""),
			wantErr:  `the CHECK of column "n" is NOT ENFORCED, which Atlas HCL cannot represent`,
		},
		{
			name:     "a foreign key not enforced",
			database: clauseDatabase(false, true, ""),
			wantErr:  `the foreign key of column "p_id" is NOT ENFORCED, which Atlas HCL cannot represent`,
		},
		{
			name:     "MATCH FULL",
			database: clauseDatabase(false, false, "FULL"),
			wantErr:  `the foreign key of column "p_id" is MATCH FULL, which Atlas HCL cannot represent`,
		},
		{
			name: "a table constraint",
			database: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
				Constraints: []schemamodel.Constraint{{
					StructName: "T", Table: "t", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
					NotEnforced: true,
				}},
			},
			wantErr: `CHECK "t_id_positive" is NOT ENFORCED, which Atlas HCL cannot represent`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.database, "postgres", "public")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, atlashclrender.Result{})
		})
	}
}

// TestRender_EnforcementAndMatch_HappyPath is the control: the same
// constraints, checked and MATCH SIMPLE, are written.
func TestRender_EnforcementAndMatch_HappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(clauseDatabase(false, false, ""), "postgres", "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `foreign_key "c_p_fkey"`)
}
