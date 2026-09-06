package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_GenerateMigrationAST_Grants(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantOptionsAdded: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "REFERENCES", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantsAdded: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	c.Assert(lines, qt.DeepEquals, []string{
		"REVOKE DELETE ON TABLE users FROM app_role;",
		"REVOKE GRANT OPTION FOR UPDATE ON TABLE users FROM app_role;",
		"GRANT USAGE ON SCHEMA public TO app_role;",
		"GRANT SELECT ON TABLE users TO app_role WITH GRANT OPTION;",
		"GRANT REFERENCES ON TABLE users TO app_role WITH GRANT OPTION;",
	})
}
