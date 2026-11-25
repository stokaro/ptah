package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// denyPipelineDesired is dbo.t with the grants and denies given, and no role:
// the grantee is one the schema does not manage.
func denyPipelineDesired(grants, denies []schemamodel.Grant) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "dbo"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "name", Type: "NVARCHAR(20)", Nullable: true},
			{StructName: "T", Name: "secret", Type: "NVARCHAR(20)", Nullable: true},
		},
		Grants:       grants,
		DeniedGrants: denies,
	}
	schemamodel.Finalize(db)
	return db
}

// splitTableGrant is the SQL Server read of `GRANT SELECT ON dbo.t TO grantee`
// followed by `GRANT SELECT (name) ON dbo.t TO grantee WITH GRANT OPTION`: a
// grant on each column, as the table grant cannot be read whole beside the
// column row.
func splitTableGrant(grantee string) *catalog.Database {
	row := func(column string, option bool) catalog.Grant {
		return catalog.Grant{
			Role: grantee, Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: column,
			WithOption: option, GrantedBy: "dbo",
		}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "dbo", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "name", DataType: "nvarchar(20)", IsNullable: "YES"},
			{Name: "secret", DataType: "nvarchar(20)", IsNullable: "YES"},
		}}},
		Grants: []catalog.Grant{row("id", false), row("name", true), row("secret", false)},
	}
}

// TestPlan_SQLServerDenyOverAReadSplitGrant_HappyPath drives the comparison
// and the planner over a table grant the SQL Server read reports as a grant
// on each column. A DENY on the table takes those grants, and the plan gives
// back only the column grants the schema declares: the split rows of a
// grantee the schema does not manage are not grants the schema keeps, and
// granting them back would undo the DENY the schema asks for.
func TestPlan_SQLServerDenyOverAReadSplitGrant_HappyPath(t *testing.T) {
	deny := schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}
	name := schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", Columns: []string{"name"}}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    []string
	}{
		{
			name:    "the deny alone",
			desired: denyPipelineDesired(nil, []schemamodel.Grant{deny}),
			want:    []string{"DENY SELECT ON [dbo].[t] TO [r] CASCADE"},
		},
		{
			name:    "the deny and a column grant",
			desired: denyPipelineDesired([]schemamodel.Grant{name}, []schemamodel.Grant{deny}),
			want:    []string{"DENY SELECT ON [dbo].[t] TO [r] CASCADE", "GRANT SELECT ([name]) ON [dbo].[t] TO [r]"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			diff, err := schemadiff.CompareWithDialect(c.Context(), test.desired, splitTableGrant("r"), "sqlserver", runtime)
			c.Assert(err, qt.IsNil)

			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "sqlserver")

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
