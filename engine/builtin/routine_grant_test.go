package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
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

			sql, err := builtin.RenderSQL(platform.Postgres, test.node)

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
			name:    "SQL Server",
			dialect: platform.SQLServer,
			node:    ast.NewGrantPrivilege("app", "ROUTINE", "purge", []string{"EXECUTE"}),
			wantErr: `(?s).*GRANT ON ROUTINE purge: privileges on routines are modeled for PostgreSQL, MySQL and MariaDB only, not sqlserver.*`,
		},
		{
			name:    "Oracle",
			dialect: platform.Oracle,
			node:    ast.NewRevokePrivilege("app", "FUNCTION", "purge", []string{"EXECUTE"}),
			wantErr: `(?s).*REVOKE ON FUNCTION purge: privileges on routines are modeled for PostgreSQL, MySQL and MariaDB only, not oracle.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderSQL_MySQLRoutinePrivilege_HappyPath pins the MySQL-family spelling
// of a privilege on a function or a procedure: the kind and the name alone,
// with no argument list, which the server refuses as a syntax error. ALL
// grants EXECUTE and ALTER ROUTINE there.
func TestRenderSQL_MySQLRoutinePrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name:    "a grant on a function",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "FUNCTION", "f1", []string{"EXECUTE"}),
			want:    "GRANT EXECUTE ON FUNCTION `f1` TO `app`;\n",
		},
		{
			name:    "a grant with the grant option on a procedure of a named database",
			dialect: platform.MariaDB,
			node:    ast.NewGrantPrivilege("app", "PROCEDURE", "shop.p1", []string{"EXECUTE", "ALTER ROUTINE"}).SetWithOption(true),
			want:    "GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `shop`.`p1` TO `app` WITH GRANT OPTION;\n",
		},
		{
			name:    "a revoke of ALL on a procedure",
			dialect: platform.MySQL,
			node:    ast.NewRevokePrivilege("app", "procedure", "p1", []string{"ALL"}),
			want:    "REVOKE ALL ON PROCEDURE `p1` FROM `app`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_MySQLRoutinePrivilege_FailurePath pins the routine targets and
// privileges the MySQL family cannot name, each refused before the server
// sees a statement: ROUTINE does not say which kind to write, an argument list
// is a syntax error, a table privilege on a routine is ERROR 1144, MariaDB's
// SHOW CREATE ROUTINE on a routine is not read back, and EXECUTE or ALTER
// ROUTINE on a target named as a table is ERROR 1144.
func TestRenderSQL_MySQLRoutinePrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "ROUTINE",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "ROUTINE", "f1", []string{"EXECUTE"}),
			wantErr: `unsupported feature: GRANT ON ROUTINE f1: mysql names a routine as FUNCTION or PROCEDURE, and ROUTINE does not say which`,
		},
		{
			name:    "an argument list",
			dialect: platform.MariaDB,
			node:    ast.NewRevokePrivilege("app", "FUNCTION", "f1", []string{"EXECUTE"}).SetArguments("int"),
			wantErr: `unsupported feature: REVOKE ON FUNCTION f1\(int\): mariadb names a routine by its name alone: leave out the argument types`,
		},
		{
			name:    "a table privilege on a routine",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "PROCEDURE", "p1", []string{"SELECT"}),
			wantErr: `unsupported feature: GRANT SELECT ON PROCEDURE p1: on mysql only EXECUTE and ALTER ROUTINE are modeled on a routine`,
		},
		{
			name:    "SHOW CREATE ROUTINE on a MariaDB routine",
			dialect: platform.MariaDB,
			node:    ast.NewGrantPrivilege("app", "FUNCTION", "f1", []string{"SHOW CREATE ROUTINE"}),
			wantErr: `unsupported feature: GRANT SHOW CREATE ROUTINE ON FUNCTION f1: on mariadb only EXECUTE and ALTER ROUTINE are modeled on a routine`,
		},
		{
			name:    "EXECUTE on a table",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "TABLE", "rt", []string{"SELECT", "EXECUTE"}),
			wantErr: `unsupported feature: GRANT EXECUTE ON rt: mysql grants EXECUTE on a routine or a database, and a target named ` +
				`without FUNCTION or PROCEDURE is a table: write ON FUNCTION rt or ON PROCEDURE rt`,
		},
		{
			name:    "ALTER ROUTINE on a target with no kind",
			dialect: platform.MariaDB,
			node:    ast.NewRevokePrivilege("app", "", "rt", []string{"ALTER ROUTINE"}),
			wantErr: `unsupported feature: REVOKE ALTER ROUTINE ON rt: mariadb grants ALTER ROUTINE on a routine or a database, and a target named ` +
				`without FUNCTION or PROCEDURE is a table: write ON FUNCTION rt or ON PROCEDURE rt`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderSQL_MySQLDatabaseGrant_HappyPath pins `ON *`, which names the
// database the session selected, for a grant on the database itself.
func TestRenderSQL_MySQLDatabaseGrant_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name:    "a grant on MySQL",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "DATABASE", "", []string{"INSERT"}).SetWithOption(true),
			want:    "GRANT INSERT ON * TO `app` WITH GRANT OPTION;\n",
		},
		{
			name:    "a revoke on MariaDB",
			dialect: platform.MariaDB,
			node:    ast.NewRevokePrivilege("app", "DATABASE", "", []string{"EXECUTE"}),
			want:    "REVOKE EXECUTE ON * FROM `app`;\n",
		},
		{
			name:    "a named database keeps its name",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "SCHEMA", "shop", []string{"SELECT"}),
			want:    "GRANT SELECT ON `shop`.* TO `app`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
