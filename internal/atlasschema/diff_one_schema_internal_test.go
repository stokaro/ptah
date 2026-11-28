package atlasschema

// White-box testing required: which database side of a diff was read at one
// schema is decided on the states a live read produces, and the only exported
// path to it opens a database. The engine tests under integration/ drive that
// path; these name the decision without a server.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// oneSchemaDatabase is a database side read at schema.
func oneSchemaDatabase(schema string) atlassource.State {
	return atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: schema}
}

// listedDatabase is a database side read at schema, whose read listed outside
// as the schemas the database holds beyond it.
func listedDatabase(schema string, outside []string) atlassource.State {
	state := oneSchemaDatabase(schema)
	state.DB = &catalog.Database{SchemasOutOfScope: outside}
	return state
}

// declaringFile is a file side declaring a table in each of schemas.
func declaringFile(schemas ...string) atlassource.State {
	desired := &schemamodel.Database{}
	for _, schema := range schemas {
		desired.Tables = append(desired.Tables, schemamodel.Table{Name: "t", Schema: schema})
	}
	return atlassource.State{Kind: atlassource.KindLocalFile, Schema: desired}
}

func TestReportOneSchemaReads_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		requested []string
		from, to  atlassource.State
		want      string
	}{
		{
			name: "a SQL Server database beside a file declaring another schema", dialect: platform.SQLServer,
			from: oneSchemaDatabase("dbo"), to: declaringFile("DBO", "app"),
			want: `Note: the --from database was read at schema "dbo" alone; pass --schemas to compare others. ` +
				`--to declares objects in "app", which this diff compares as absent from the database.` + "\n",
		},
		{
			name: "a SQL Server database on the --to side", dialect: platform.SQLServer,
			from: declaringFile(), to: oneSchemaDatabase("app"),
			want: `Note: the --to database was read at schema "app" alone; pass --schemas to compare others.` + "\n",
		},
		{
			name: "a SQL Server database holding another schema", dialect: platform.SQLServer,
			from: listedDatabase("dbo", []string{"app"}), to: declaringFile("dbo"),
			want: `Note: the --from database was read at schema "dbo" alone; pass --schemas to compare others.` + "\n",
		},
		{
			name: "a SQL Server database holding no other schema", dialect: platform.SQLServer,
			from: listedDatabase("dbo", make([]string, 0)), to: declaringFile("dbo", "app"),
		},
		{
			name: "schemas the run names", dialect: platform.SQLServer, requested: []string{"dbo", "app"},
			from: oneSchemaDatabase("dbo"), to: declaringFile("app"),
		},
		{
			name: "a PostgreSQL URL, whose search_path selects the scope", dialect: platform.Postgres,
			from: oneSchemaDatabase("public"), to: declaringFile("app"),
		},
		{
			name: "two files", dialect: platform.SQLServer,
			from: declaringFile("app"), to: declaringFile("dbo"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer

			reportOneSchemaReads(&diagnostics, oneSchemaReads(test.dialect, test.requested, test.from, test.to))

			c.Assert(diagnostics.String(), qt.Equals, test.want)
		})
	}
}
