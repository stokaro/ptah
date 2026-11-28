package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// unqualifiedNamesDeclaration declares a table, a view, a function and a
// sequence, each named as names gives it, so a name with a schema is written
// qualified.
func unqualifiedNamesDeclaration(table, schema, view, function, sequence string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables:    []schemamodel.Table{{StructName: "T", Name: table, Schema: schema}},
		Fields:    []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT"}},
		Views:     []schemamodel.View{{Name: view, Body: "SELECT 1 AS one"}},
		Functions: []schemamodel.Function{{Name: function, Returns: "int", Language: "sql", Body: "BEGIN RETURN 1 END"}},
		Sequences: []schemamodel.Sequence{{Name: sequence, Schema: schema}},
	}
}

// urlSchemaApp is a SQL Server connection reading app whose server creates an
// unqualified name in dbo, as a URL naming schema=app does for sa.
var urlSchemaApp = catalog.ServerInfo{Dialect: "sqlserver", Schema: "app", UnqualifiedSchema: "dbo"}

// TestCompareWithDatabaseInfo_UnqualifiedNames_FailurePath pins the refusal
// of a declaration that writes a name without a schema on a connection whose
// server creates it in another schema than the one it reads: every such name,
// sorted, and both schemas.
func TestCompareWithDatabaseInfo_UnqualifiedNames_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(),
		unqualifiedNamesDeclaration("t1", "", "v1", "f1", "s1"), &catalog.Database{}, urlSchemaApp, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `.*the desired schema writes "f1", "s1", "t1", "v1" without a schema: `+
		`this connection reads schema "app", and the server creates such a name in "dbo", the default schema of the `+
		`connection's user, so every run would plan it again; qualify each name with its schema, or leave the schema `+
		`parameter out of the URL for a user whose default schema is "app"`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfo_UnqualifiedNames_HappyPath is the control: the
// same names qualified on that connection, and the unqualified ones on a
// connection whose server creates them where it reads, are compared.
func TestCompareWithDatabaseInfo_UnqualifiedNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		info     catalog.ServerInfo
		wantAdds int
	}{
		{"qualified names", unqualifiedNamesDeclaration("t1", "app", "app.v1", "app.f1", "s1"), urlSchemaApp, 1},
		{"a connection that creates them where it reads", unqualifiedNamesDeclaration("t1", "", "v1", "f1", "s1"),
			catalog.ServerInfo{Dialect: "sqlserver", Schema: "app"}, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.desired, &catalog.Database{}, test.info, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded, qt.HasLen, test.wantAdds)
		})
	}
}
