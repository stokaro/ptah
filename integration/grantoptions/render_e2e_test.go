//go:build integration

package grantoptions_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// renderOptionDocument drives schema render on a SQL file. Its output is the
// script applied to existing objects, so progress messages stay on stderr.
func renderOptionDocument(c *qt.C, dialect, document string) string {
	c.Helper()
	result := ptah(c, "schema", "render", "--dialect", dialect, "--schema-file", schemaFile(c, document))
	c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
	return result.Stdout
}

// A rendered declaration takes an option already held on a table, a column
// or a routine. A plain GRANT without the option would leave it held.
func TestSchemaRenderPostgresTakesDeclaredGrantOptionsE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		query  string
	}{
		{name: "table", target: "SELECT ON TABLE t48", query: "SELECT has_table_privilege($1, 't48', 'SELECT'), has_table_privilege($1, 't48', 'SELECT WITH GRANT OPTION')"},
		{name: "column", target: "SELECT (id) ON TABLE t48", query: "SELECT has_column_privilege($1, 't48', 'id', 'SELECT'), has_column_privilege($1, 't48', 'id', 'SELECT WITH GRANT OPTION')"},
		{name: "function", target: "EXECUTE ON FUNCTION f6596(integer)", query: "SELECT has_function_privilege($1, 'f6596(integer)', 'EXECUTE'), has_function_privilege($1, 'f6596(integer)', 'EXECUTE WITH GRANT OPTION')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := postgresRole(c)
			dev := postgresDatabase(c)
			dev.exec(c, []string{
				"CREATE TABLE t48 (id integer PRIMARY KEY, kept integer)",
				"CREATE FUNCTION f6596(integer) RETURNS integer LANGUAGE sql AS 'SELECT $1'",
				"GRANT " + test.target + " TO " + role + " WITH GRANT OPTION",
			})
			var held, option bool
			c.Assert(dev.conn.QueryRowContext(c.Context(), test.query, role).Scan(&held, &option), qt.IsNil)
			c.Assert(held, qt.IsTrue)
			c.Assert(option, qt.IsTrue)

			rendered := renderOptionDocument(c, "postgres", "GRANT "+test.target+" TO "+role+" WITH GRANT OPTION;\n"+
				"REVOKE GRANT OPTION FOR "+test.target+" FROM "+role+";\n")
			statements := sqlutil.SplitSQLStatementsForDialect(rendered, "postgres")
			c.Assert(statements, qt.HasLen, 2)
			dev.exec(c, statements)

			c.Assert(dev.conn.QueryRowContext(c.Context(), test.query, role).Scan(&held, &option), qt.IsNil)
			c.Assert(held, qt.IsTrue)
			c.Assert(option, qt.IsFalse)
		})
	}
}

// Revoking the table option must precede a declared column option. The
// column's ability to grant onward is read back independently of the table.
func TestSchemaRenderPostgresKeepsAColumnOptionAfterATableRevokeE2E(t *testing.T) {
	c := qt.New(t)
	role := postgresRole(c)
	dev := postgresDatabase(c)
	dev.exec(c, []string{
		"CREATE TABLE t48 (id integer PRIMARY KEY, kept integer)",
		"GRANT SELECT ON t48 TO " + role + " WITH GRANT OPTION",
	})
	document := "GRANT SELECT ON t48 TO " + role + " WITH GRANT OPTION;\n" +
		"REVOKE GRANT OPTION FOR SELECT ON t48 FROM " + role + ";\n" +
		"GRANT SELECT (kept) ON t48 TO " + role + " WITH GRANT OPTION;\n"

	rendered := renderOptionDocument(c, "postgres", document)
	dev.exec(c, sqlutil.SplitSQLStatementsForDialect(rendered, "postgres"))

	var tableOption, columnOption bool
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT has_table_privilege($1, 't48', 'SELECT WITH GRANT OPTION'), "+
		"has_column_privilege($1, 't48', 'kept', 'SELECT WITH GRANT OPTION')", role).Scan(&tableOption, &columnOption), qt.IsNil)
	c.Assert(tableOption, qt.IsFalse)
	c.Assert(columnOption, qt.IsTrue)
}

// Both writer paths preserve the refusal and add a remedy when another role
// holds a dependent grant. The failed apply leaves both grants in place.
func TestSchemaApplyPostgresExplainsDependentGrantOptionsE2E(t *testing.T) {
	for _, mode := range []string{"all", "none"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			role := postgresRole(c)
			dependent := postgresRole(c)
			dev := postgresDatabase(c)
			dev.exec(c, []string{
				"CREATE TABLE t48 (id integer PRIMARY KEY)",
				"GRANT SELECT ON t48 TO " + role + " WITH GRANT OPTION",
				"SET ROLE " + role + "; GRANT SELECT ON t48 TO " + dependent + "; RESET ROLE",
			})
			statements := "GRANT SELECT ON t48 TO {role} WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR SELECT ON t48 FROM {role};\n"

			document := "CREATE TABLE t48 (id integer PRIMARY KEY);\n" + strings.ReplaceAll(statements, "{role}", role)
			result := ptah(c, "schema", "apply", "--db-url", dev.url, "--schema-file", schemaFile(c, document), "--auto-approve", "--tx-mode", mode)

			c.Assert(result.ExitCode, qt.Equals, 2, outcome(result))
			c.Assert(result.Stderr, qt.Contains, "2BP01")
			c.Assert(result.Stderr, qt.Contains, "revoke the dependent grants first, then retry the grant-option revoke")
			c.Assert(selectOnT48(c, dev, role), qt.Equals, "SELECT true OPTION true")
			c.Assert(selectOnT48(c, dev, dependent), qt.Equals, "SELECT true OPTION false")
		})
	}
}

// SQL Server can remove the base permission when REVOKE GRANT OPTION FOR
// runs on an ordinary grant. Rendering the revoke first leaves the declared
// ordinary grant on every starting state, at object, column and schema scope.
func TestSchemaRenderSQLServerKeepsBasePermissionsAfterOptionRevokesE2E(t *testing.T) {
	for _, scope := range []struct {
		name   string
		target string
		column string
		class  int
	}{
		{name: "table", target: "SELECT ON dbo.t1", column: "", class: 1},
		{name: "column", target: "SELECT (name) ON dbo.t1", column: "name", class: 1},
		{name: "schema", target: "SELECT ON SCHEMA::dbo", column: "", class: 3},
	} {
		for _, initial := range []struct {
			name string
			held string
		}{
			{name: "absent", held: "SELECT 1"},
			{name: "ordinary", held: "GRANT {target} TO [{role}]"},
			{name: "grantable", held: "GRANT {target} TO [{role}] WITH GRANT OPTION"},
		} {
			t.Run(fmt.Sprintf("%s/%s", scope.name, initial.name), func(t *testing.T) {
				c := qt.New(t)
				server := sqlServerDatabase(c, strings.ReplaceAll(initial.held, "{target}", scope.target))
				document := "GRANT " + scope.target + " TO [" + server.role + "] WITH GRANT OPTION;\n" +
					"REVOKE GRANT OPTION FOR " + scope.target + " FROM [" + server.role + "];\n"

				rendered := renderOptionDocument(c, "sqlserver", document)
				statements := sqlutil.SplitSQLStatementsForDialect(rendered, "sqlserver")
				c.Assert(statements, qt.HasLen, 2)
				for _, statement := range statements {
					_, err := server.conn.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}

				rows := grantRows(c, server.conn, `SELECT p.state FROM sys.database_permissions p
					WHERE p.grantee_principal_id = USER_ID(@p1) AND p.class = @p2
					AND p.major_id = CASE p.class WHEN 3 THEN SCHEMA_ID('dbo') ELSE OBJECT_ID('dbo.t1') END
					AND p.permission_name = 'SELECT'
					AND CASE p.class WHEN 1 THEN COALESCE(COL_NAME(p.major_id, p.minor_id), '') ELSE '' END = @p3`,
					server.role, scope.class, scope.column)
				c.Assert(rows, qt.DeepEquals, []string{"G"})
				_, err := server.conn.ExecContext(c.Context(), "EXECUTE AS USER = 'reader'; SELECT name FROM dbo.t1; REVERT")
				c.Assert(err, qt.IsNil)
			})
		}
	}
}

// The table option is removed before a declared column option is restored.
// Reading both catalog rows distinguishes the table's ordinary grant from
// the column's own grant option; a successful SELECT also proves the base
// permission survives on the other column.
func TestSchemaRenderSQLServerKeepsAColumnOptionAfterATableRevokeE2E(t *testing.T) {
	c := qt.New(t)
	server := sqlServerDatabase(c, "GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION")
	document := "GRANT SELECT ON dbo.t1 TO reader WITH GRANT OPTION;\n" +
		"REVOKE GRANT OPTION FOR SELECT ON dbo.t1 FROM reader;\n" +
		"GRANT SELECT (name) ON dbo.t1 TO reader WITH GRANT OPTION;\n"

	rendered := renderOptionDocument(c, "sqlserver", document)
	statements := sqlutil.SplitSQLStatementsForDialect(rendered, "sqlserver")
	c.Assert(statements, qt.HasLen, 3)
	server.exec(c, statements)

	rows := grantRows(c, server.conn, `SELECT p.state + ' ' + COALESCE(COL_NAME(p.major_id, p.minor_id), '<table>')
		FROM sys.database_permissions p
		WHERE p.grantee_principal_id = USER_ID(@p1) AND p.class = 1
		AND p.major_id = OBJECT_ID('dbo.t1') AND p.permission_name = 'SELECT'
		ORDER BY p.minor_id`, server.role)
	c.Assert(rows, qt.DeepEquals, []string{"G <table>", "W name"})
	_, err := server.conn.ExecContext(c.Context(), "EXECUTE AS USER = 'reader'; SELECT id, name FROM dbo.t1; REVERT")
	c.Assert(err, qt.IsNil)
}
