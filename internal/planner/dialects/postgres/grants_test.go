package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_Grants(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		GrantsRemoved: []types.GrantRef{
			{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantOptionsRevoked: []types.GrantRef{
			{Role: "app_role", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantOptionsAdded: []types.GrantRef{
			{Role: "app_role", Privilege: "REFERENCES", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantsAdded: []types.GrantRef{
			{Role: "app_role", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
	}

	sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(diff, &goschema.Database{})...)

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
