package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// sqlServerDeniedRow is a catalog permission of reader on dbo.t1 or app.
// R is a column partial revoke, which must never be expanded as a DENY.
func sqlServerDeniedRow(kind, column, grantor, state string) catalog.Grant {
	row := sqlServerReadGrant(kind, "t1", column, grantor, state == "W")
	row.Schema, row.ObjectOwner = "dbo", "dbo"
	row.IsPartialRevoke, row.Denied = state == "D" || state == "R", state == "D"
	if kind == "SCHEMA" {
		row.Schema, row.ObjectName = "", "app"
	}
	return row
}

// A complete desired read removes every matching DENY grantor. A column
// DENY under a kept table grant of the same grantor is cleared with GRANT AS,
// so the server does not replace it with a column REVOKE row.
func TestGenerateSchemaDiffSQLStatements_SQLServerRevokesEveryDenialGrantor_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, declared string
		held           []catalog.Grant
		want           []string
	}{
		{
			name: "every object denial",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "", "dbo", "D"), sqlServerDeniedRow("TABLE", "", "other", "D")},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader] AS [dbo]", "REVOKE SELECT ON [dbo].[t1] FROM [reader] AS [other]"},
		},
		{
			name: "every schema denial",
			held: []catalog.Grant{sqlServerDeniedRow("SCHEMA", "", "dbo", "D"), sqlServerDeniedRow("SCHEMA", "", "other", "D")},
			want: []string{"REVOKE SELECT ON SCHEMA::[app] FROM [reader] AS [dbo]", "REVOKE SELECT ON SCHEMA::[app] FROM [reader] AS [other]"},
		},
		{
			name: "every column denial",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "id", "dbo", "D"), sqlServerDeniedRow("TABLE", "id", "other", "D")},
			want: []string{"REVOKE SELECT ([id]) ON [dbo].[t1] FROM [reader] AS [dbo]", "REVOKE SELECT ([id]) ON [dbo].[t1] FROM [reader] AS [other]"},
		},
		{
			name: "a column denial under its grantor's table grant", declared: "GRANT SELECT ON dbo.t1 TO reader;",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "", "other", "G"), sqlServerDeniedRow("TABLE", "id", "other", "D")},
			want: []string{"GRANT SELECT ([id]) ON [dbo].[t1] TO [reader] AS [other]"},
		},
		{
			name: "a column denial under its grantor's grantable table grant", declared: "GRANT SELECT ON dbo.t1 TO reader WITH GRANT OPTION;",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "", "other", "W"), sqlServerDeniedRow("TABLE", "id", "other", "D")},
			want: []string{"GRANT SELECT ([id]) ON [dbo].[t1] TO [reader] WITH GRANT OPTION AS [other]"},
		},
		{
			name: "two column denials under each grantor's table grant", declared: "GRANT SELECT ON dbo.t1 TO reader;",
			held: []catalog.Grant{
				sqlServerDeniedRow("TABLE", "", "dbo", "G"), sqlServerDeniedRow("TABLE", "", "other", "G"),
				sqlServerDeniedRow("TABLE", "id", "dbo", "D"), sqlServerDeniedRow("TABLE", "id", "other", "D"),
			},
			want: []string{"GRANT SELECT ([id]) ON [dbo].[t1] TO [reader] AS [dbo]", "GRANT SELECT ([id]) ON [dbo].[t1] TO [reader] AS [other]"},
		},
		{
			name: "another grantor's table grant is not a matching base", declared: "GRANT SELECT ON dbo.t1 TO reader;",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "", "dbo", "G"), sqlServerDeniedRow("TABLE", "id", "other", "D")},
			want: []string{"REVOKE SELECT ([id]) ON [dbo].[t1] FROM [reader] AS [other]"},
		},
		{
			name: "a partial revoke is not another denial",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "id", "dbo", "D"), sqlServerDeniedRow("TABLE", "id", "other", "R")},
			want: []string{"REVOKE SELECT ([id]) ON [dbo].[t1] FROM [reader] AS [dbo]"},
		},
		{
			name: "the grantor name is decoded", declared: "GRANT SELECT ON dbo.t1 TO reader;",
			held: []catalog.Grant{sqlServerDeniedRow("TABLE", "", "[grantor]", "G"), sqlServerDeniedRow("TABLE", "id", "[grantor]", "D")},
			want: []string{"GRANT SELECT ([id]) ON [dbo].[t1] TO [reader] AS [[grantor]]]"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE SCHEMA app; CREATE TABLE dbo.t1 (id INT); CREATE ROLE reader; "+test.declared),
				platform.SQLServer, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			current, err := goschematodb.ToDBSchema(c.Context(), &desired, platform.SQLServer, runtime)
			c.Assert(err, qt.IsNil)
			current.Grants = test.held
			diff, err := schemadiff.CompareWithOptions(c.Context(), &desired, current,
				&config.CompareOptions{Dialect: platform.SQLServer, DesiredGrantsComplete: true}, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.DeniesRemoved, qt.HasLen, 1)

			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
