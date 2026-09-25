package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// TestRenderSQL_RoutinePrivilege_HappyPath pins the PostgreSQL spelling of a
// privilege on a function or procedure. The argument types follow the name,
// because PostgreSQL resolves the routine by them, and PUBLIC stays the bare
// keyword where a role name is quoted.
func TestRenderSQL_RoutinePrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a grant on a function",
			node: ast.NewGrantPrivilege("wpmgr_app", "FUNCTION", "purge_workspace", []string{"EXECUTE"}).
				SetArguments("uuid"),
			want: `GRANT EXECUTE ON FUNCTION "purge_workspace"(uuid) TO "wpmgr_app";` + "\n",
		},
		{
			name: "a revoke from PUBLIC on a procedure with no arguments",
			node: ast.NewRevokePrivilege("PUBLIC", "PROCEDURE", "archive", []string{"EXECUTE"}),
			want: `REVOKE EXECUTE ON PROCEDURE "archive"() FROM PUBLIC;` + "\n",
		},
		{
			name: "the catalog spelling with parameter names",
			node: ast.NewRevokePrivilege("PUBLIC", "FUNCTION", "public.purge", []string{"EXECUTE"}).
				SetArguments("p_id uuid, at timestamp with time zone"),
			want: `REVOKE EXECUTE ON FUNCTION "public"."purge"(p_id uuid, at timestamp with time zone) FROM PUBLIC;` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.Postgres, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_RoutinePrivilege_FailurePath pins that a renderer with no
// routine grant spelling refuses the node by name. Printing the routine name
// where the grammar expects a table would hand the server a statement about a
// different object.
func TestRenderSQL_RoutinePrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "FUNCTION", "purge", []string{"EXECUTE"}).SetArguments("uuid"),
			wantErr: `(?s).*GRANT ON FUNCTION purge: privileges on routines are modeled for PostgreSQL only, not mysql.*`,
		},
		{
			name:    "MariaDB",
			dialect: platform.MariaDB,
			node:    ast.NewRevokePrivilege("app", "PROCEDURE", "archive", []string{"EXECUTE"}),
			wantErr: `(?s).*REVOKE ON PROCEDURE archive: privileges on routines are modeled for PostgreSQL only, not mariadb.*`,
		},
		{
			name:    "SQL Server",
			dialect: platform.SQLServer,
			node:    ast.NewGrantPrivilege("app", "ROUTINE", "purge", []string{"EXECUTE"}),
			wantErr: `(?s).*GRANT ON ROUTINE purge: privileges on routines are modeled for PostgreSQL only, not sqlserver.*`,
		},
		{
			name:    "Oracle",
			dialect: platform.Oracle,
			node:    ast.NewRevokePrivilege("app", "FUNCTION", "purge", []string{"EXECUTE"}),
			wantErr: `(?s).*REVOKE ON FUNCTION purge: privileges on routines are modeled for PostgreSQL only, not oracle.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
