package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// A privilege of more than one word is read as the longest name the dialect's
// grammar lists that its words spell. A T-SQL grant on a schema names the
// class before `::`, and a MySQL grant on a database names it as `db.*`. Each
// row is a statement `ptah db read` writes, or one a schema author writes the
// same way. Names keep the quotes they were written with.
func TestParserGrantPrivilegeWords_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, sql     string
		privileges             []string
		objectType, objectName string
	}{
		{
			name: "MySQL SHOW VIEW", dialect: platform.MySQL, sql: "GRANT SELECT, SHOW VIEW ON `d`.`t1` TO `r`;",
			privileges: []string{"SELECT", "SHOW VIEW"}, objectType: "TABLE", objectName: "`d`.`t1`",
		},
		{
			name: "MySQL privileges on a database", dialect: platform.MySQL, sql: "GRANT LOCK TABLES, CREATE TEMPORARY TABLES ON `d`.* TO `r`;",
			privileges: []string{"LOCK TABLES", "CREATE TEMPORARY TABLES"}, objectType: "SCHEMA", objectName: "`d`",
		},
		{
			name: "MySQL CREATE beside CREATE VIEW", dialect: platform.MySQL, sql: "GRANT CREATE, CREATE VIEW, CREATE ROUTINE ON `d`.* TO r;",
			privileges: []string{"CREATE", "CREATE VIEW", "CREATE ROUTINE"}, objectType: "SCHEMA", objectName: "`d`",
		},
		{
			name: "MySQL ALTER ROUTINE on a routine", dialect: platform.MySQL, sql: "GRANT ALTER ROUTINE, EXECUTE ON PROCEDURE `d`.`p1` TO r;",
			privileges: []string{"ALTER ROUTINE", "EXECUTE"}, objectType: "PROCEDURE", objectName: "`d`.`p1`",
		},
		{
			name: "MySQL EXECUTE on a function by its name alone", dialect: platform.MySQL, sql: "GRANT EXECUTE ON FUNCTION f1 TO r;",
			privileges: []string{"EXECUTE"}, objectType: "FUNCTION", objectName: "f1",
		},
		{
			name: "MySQL privileges on the database the session selects", dialect: platform.MySQL, sql: "GRANT INSERT, EXECUTE ON * TO r;",
			privileges: []string{"INSERT", "EXECUTE"}, objectType: "DATABASE", objectName: "",
		},
		{
			name: "MySQL words in lower case across lines", dialect: platform.MySQL, sql: "grant show\n  view on t1 to r;",
			privileges: []string{"SHOW VIEW"}, objectType: "TABLE", objectName: "t1",
		},
		{
			name: "MariaDB DELETE HISTORY and SHOW CREATE ROUTINE", dialect: platform.MariaDB, sql: "GRANT DELETE HISTORY, SHOW CREATE ROUTINE ON `d`.* TO r;",
			privileges: []string{"DELETE HISTORY", "SHOW CREATE ROUTINE"}, objectType: "SCHEMA", objectName: "`d`",
		},
		{
			name: "Oracle privileges that begin with ON", dialect: platform.Oracle, sql: "GRANT ON COMMIT REFRESH, QUERY REWRITE ON APP.T1 TO R;",
			privileges: []string{"ON COMMIT REFRESH", "QUERY REWRITE"}, objectType: "TABLE", objectName: "APP.T1",
		},
		{
			name: "SQL Server permissions on an object", dialect: platform.SQLServer, sql: "GRANT TAKE OWNERSHIP, VIEW DEFINITION ON [dbo].[t1] TO [r];",
			privileges: []string{"TAKE OWNERSHIP", "VIEW DEFINITION"}, objectType: "TABLE", objectName: "[dbo].[t1]",
		},
		{
			name: "SQL Server permissions on a schema", dialect: platform.SQLServer, sql: "GRANT SELECT, VIEW CHANGE TRACKING ON SCHEMA::[dbo] TO [r];",
			privileges: []string{"SELECT", "VIEW CHANGE TRACKING"}, objectType: "SCHEMA", objectName: "[dbo]",
		},
		{
			name: "SQL Server class with spaces around the qualifier", dialect: platform.SQLServer, sql: "GRANT CREATE SEQUENCE ON SCHEMA :: dbo TO r;",
			privileges: []string{"CREATE SEQUENCE"}, objectType: "SCHEMA", objectName: "dbo",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			grant, ok := statements.Statements[0].(*ast.GrantPrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(grant.Privileges, qt.DeepEquals, test.privileges)
			c.Assert(grant.ObjectType, qt.Equals, test.objectType)
			c.Assert(grant.ObjectName, qt.Equals, test.objectName)
		})
	}
}

// A REVOKE reads its privileges and its target the way a GRANT does.
func TestParserRevokePrivilegeWords_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, sql     string
		privileges             []string
		objectType, objectName string
	}{
		{
			name: "MySQL", dialect: platform.MySQL, sql: "REVOKE SHOW VIEW ON t1 FROM r;",
			privileges: []string{"SHOW VIEW"}, objectType: "TABLE", objectName: "t1",
		},
		{
			name: "MySQL on a database", dialect: platform.MySQL, sql: "REVOKE LOCK TABLES ON `d` . * FROM r;",
			privileges: []string{"LOCK TABLES"}, objectType: "SCHEMA", objectName: "`d`",
		},
		{
			name: "MariaDB on the database the session selects", dialect: platform.MariaDB, sql: "REVOKE EXECUTE ON  *  FROM r;",
			privileges: []string{"EXECUTE"}, objectType: "DATABASE", objectName: "",
		},
		{
			name: "SQL Server", dialect: platform.SQLServer, sql: "REVOKE VIEW DEFINITION ON SCHEMA::[dbo] FROM [r];",
			privileges: []string{"VIEW DEFINITION"}, objectType: "SCHEMA", objectName: "[dbo]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			revoke, ok := statements.Statements[0].(*ast.RevokePrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(revoke.Privileges, qt.DeepEquals, test.privileges)
			c.Assert(revoke.ObjectType, qt.Equals, test.objectType)
			c.Assert(revoke.ObjectName, qt.Equals, test.objectName)
		})
	}
}

// A name the dialect's grammar does not list is read one word at a time, so
// its second word is refused where a comma or ON belongs. So is a name that
// begins a listed one and does not finish it, and a class the grammar does not
// name before `::`.
func TestParserGrantPrivilegeWords_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name: "PostgreSQL has no privilege of two words", dialect: platform.Postgres, sql: "GRANT SHOW VIEW ON t1 TO r;",
			wantErr: `line 1, column 12: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a name another dialect lists", dialect: platform.MySQL, sql: "GRANT VIEW DEFINITION ON t1 TO r;",
			wantErr: `line 1, column 12: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "the start of a listed name", dialect: platform.MySQL, sql: "GRANT CREATE TEMPORARY ON `d`.* TO r;",
			wantErr: `line 1, column 14: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a quoted first word", dialect: platform.Oracle, sql: `GRANT "ON" COMMIT REFRESH ON t1 TO r;`,
			wantErr: `line 1, column 12: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a bracketed first word", dialect: platform.SQLServer, sql: "GRANT [VIEW] DEFINITION ON t1 TO r;",
			wantErr: `line 1, column 14: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a quoted second word on MySQL", dialect: platform.MySQL, sql: "GRANT SHOW `VIEW` ON t1 TO r;",
			wantErr: `line 1, column 12: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a bracketed second word on SQL Server", dialect: platform.SQLServer, sql: "GRANT VIEW [DEFINITION] ON t1 TO r;",
			wantErr: `line 1, column 12: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a quoted third word on MySQL", dialect: platform.MySQL, sql: "GRANT CREATE TEMPORARY `TABLES` ON `d`.* TO r;",
			wantErr: `line 1, column 14: expected ',' or ON in GRANT privileges`,
		},
		{
			name: "a REVOKE of every database", dialect: platform.MariaDB, sql: "REVOKE SELECT ON * . * FROM r;",
			wantErr: `line 1, column 18: REVOKE ON \*\.\* names every database, and a schema describes one database: run it outside the schema`,
		},
		{
			name: "a REVOKE of the start of a listed name", dialect: platform.Oracle, sql: "REVOKE ON COMMIT ON t1 FROM r;",
			wantErr: `line 1, column 11: expected ',' or ON in REVOKE privileges`,
		},
		{
			name: "one colon after the class", dialect: platform.SQLServer, sql: "GRANT SELECT ON SCHEMA:dbo TO r;",
			wantErr: `line 1, column 23: expected '::' after SCHEMA in GRANT target`,
		},
		{
			name: "no schema after the class", dialect: platform.SQLServer, sql: "GRANT SELECT ON SCHEMA:: TO r;",
			wantErr: `line 1, column 26: GRANT ON SCHEMA:: names no schema: write its name after the '::'`,
		},
		{
			name: "no schema after the class in a REVOKE", dialect: platform.SQLServer, sql: "REVOKE SELECT ON SCHEMA::;",
			wantErr: `line 1, column 26: REVOKE ON SCHEMA:: names no schema: write its name after the '::'`,
		},
		{
			name: "a class SQL Server does not name", dialect: platform.SQLServer, sql: "GRANT SELECT ON TABLE::dbo.t1 TO r;",
			wantErr: `line 1, column 22: expected GRANT object name: .*`,
		},
		{
			name: "a class on PostgreSQL", dialect: platform.Postgres, sql: "GRANT USAGE ON SCHEMA::public TO r;",
			wantErr: `line 1, column 22: expected GRANT object name: .*`,
		},
		{
			name: "a database wildcard on PostgreSQL", dialect: platform.Postgres, sql: "GRANT SELECT ON d.* TO r;",
			wantErr: `line 1, column 19: expected identifier after '.' in GRANT object name: .*`,
		},
		{
			name: "the selected database on PostgreSQL", dialect: platform.Postgres, sql: "GRANT SELECT ON * TO r;",
			wantErr: `line 1, column 17: expected GRANT object name: .*`,
		},
		{
			name: "an argument list on a MySQL routine", dialect: platform.MySQL, sql: "GRANT EXECUTE ON FUNCTION f1() TO r;",
			wantErr: `line 1, column 29: GRANT \.\.\. ON FUNCTION f1\(\.\.\.\): mysql names a routine by its name alone: leave out the argument list`,
		},
		{
			name: "an argument list on a MariaDB routine in a REVOKE", dialect: platform.MariaDB, sql: "REVOKE EXECUTE ON PROCEDURE `d`.`p1` (int) FROM r;",
			wantErr: "line 1, column 38: REVOKE \\.\\.\\. ON PROCEDURE `d`.`p1`\\(\\.\\.\\.\\): mariadb names a routine by its name alone: leave out the argument list",
		},
		{
			name: "ROUTINE on MariaDB", dialect: platform.MariaDB, sql: "GRANT EXECUTE ON ROUTINE f1 TO r;",
			wantErr: `line 1, column 26: GRANT \.\.\. ON ROUTINE: mariadb names a routine as FUNCTION or PROCEDURE: write ON FUNCTION or ON PROCEDURE`,
		},
		{
			name: "every database on MySQL", dialect: platform.MySQL, sql: "GRANT SELECT ON *.* TO r;",
			wantErr: `line 1, column 17: GRANT ON \*\.\* names every database, and a schema describes one database: run it outside the schema`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
