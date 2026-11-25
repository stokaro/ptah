package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// TestLoadPath_SchemaDirectoryKeepsAColumnStatementUnderATableOne_HappyPath
// pins that a SQL Server schema directory keeps what each file declares about
// a table and its column, as the file read alone does. A statement on a whole
// table takes the column statements of its privilege, so a file holding both
// wrote the column one after it, and the server keeps both rows.
func TestLoadPath_SchemaDirectoryKeepsAColumnStatementUnderATableOne_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		privileges string
		wantGrants []schemamodel.Grant
		wantDenied []schemamodel.Grant
	}{
		{
			name:       "a column grant after a deny on its table",
			privileges: "DENY SELECT ON dbo.t TO r;\nGRANT SELECT (name) ON dbo.t TO r;\n",
			wantGrants: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", Columns: []string{"name"}}},
			wantDenied: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}},
		},
		{
			name:       "a column deny after a grant on its table",
			privileges: "GRANT SELECT ON dbo.t TO r;\nDENY SELECT (secret) ON dbo.t TO r;\n",
			wantGrants: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}},
			wantDenied: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", Columns: []string{"secret"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, map[string]string{
				"1_table.sql":      "CREATE ROLE r;\nCREATE TABLE dbo.t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), secret NVARCHAR(20));\n",
				"2_privileges.sql": test.privileges,
			})

			db, err := schemafile.LoadPath(t.Context(), dir, schemafile.Options{
				YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: "sqlserver",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(db.Grants, qt.DeepEquals, test.wantGrants)
			c.Assert(db.DeniedGrants, qt.DeepEquals, test.wantDenied)
		})
	}
}
