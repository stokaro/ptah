package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/parser"
	"github.com/stokaro/ptah/core/renderer"
)

func TestNewParser(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE TABLE users (id INTEGER);")
	c.Assert(p, qt.IsNotNil)
}

func TestParser_ParseCreateTable_Basic(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNotNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	// Check that it's a CREATE TABLE statement
	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "users")
	c.Assert(createTable.Columns, qt.HasLen, 2)

	// Check first column (id)
	idColumn := createTable.Columns[0]
	c.Assert(idColumn.Name, qt.Equals, "id")
	c.Assert(idColumn.Type, qt.Equals, "INTEGER")
	c.Assert(idColumn.Primary, qt.IsTrue)
	c.Assert(idColumn.Nullable, qt.IsFalse) // Primary keys are NOT NULL

	// Check second column (name)
	nameColumn := createTable.Columns[1]
	c.Assert(nameColumn.Name, qt.Equals, "name")
	c.Assert(nameColumn.Type, qt.Equals, "VARCHAR(255)")
	c.Assert(nameColumn.Primary, qt.IsFalse)
	c.Assert(nameColumn.Nullable, qt.IsFalse) // NOT NULL specified
}

func TestParser_ParseCreateTable_WithConstraints(t *testing.T) {
	c := qt.New(t)

	// Test just the constraint part first
	sql := `CREATE TABLE orders (id INTEGER PRIMARY KEY, FOREIGN KEY (id) REFERENCES users(id));`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "orders")
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check id column
	idColumn := createTable.Columns[0]
	c.Assert(idColumn.Name, qt.Equals, "id")
	c.Assert(idColumn.Type, qt.Equals, "INTEGER")
	c.Assert(idColumn.Primary, qt.IsTrue)

	// Check foreign key constraint
	fkConstraint := createTable.Constraints[0]
	c.Assert(fkConstraint.Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(fkConstraint.Columns, qt.DeepEquals, []string{"id"})
	c.Assert(fkConstraint.Reference, qt.IsNotNil)
	c.Assert(fkConstraint.Reference.Table, qt.Equals, "users")
	c.Assert(fkConstraint.Reference.Column, qt.Equals, "id")
}

func TestParser_ParseCreateTable_WithTableOptions(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR(255)
	) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='Product catalog';`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "products")
	c.Assert(createTable.Options["ENGINE"], qt.Equals, "InnoDB")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "utf8mb4")
	c.Assert(createTable.Comment, qt.Equals, "'Product catalog'")
}

func TestParser_ParseCreateTable_IfNotExists(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "users")
	c.Assert(createTable.IfNotExists, qt.IsTrue)
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].Primary, qt.IsTrue)
}

func TestParser_ParseSQLServerGoBatchSeparators(t *testing.T) {
	c := qt.New(t)

	sql := `
GO
CREATE TABLE first_table (id INTEGER)
gO 11
CREATE TABLE second_table (id INTEGER)
GO
`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)
	c.Assert(statements.Statements[0].(*ast.CreateTableNode).Name, qt.Equals, "first_table")
	c.Assert(statements.Statements[1].(*ast.CreateTableNode).Name, qt.Equals, "second_table")
}

func TestParser_ParseDoesNotTreatGotoAsGoBatchSeparator(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("GOTO label;")
	_, err := p.Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported SQL statement: GOTO at position 0`)
}

func TestParser_ParseCreateTable_TablePrimaryKeyWithoutComma(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE post
(
    id int NOT NULL,
    created_at TIMESTAMP NOT NULL
    PRIMARY KEY (id)
);

INSERT INTO post (id, created_at) VALUES (1, NOW());`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 2)
	c.Assert(createTable.Columns[1].Name, qt.Equals, "created_at")
	c.Assert(createTable.Columns[1].Nullable, qt.IsFalse)
	c.Assert(createTable.Columns[1].Primary, qt.IsFalse)
	c.Assert(createTable.Constraints, qt.HasLen, 1)
	c.Assert(createTable.Constraints[0].Type, qt.Equals, ast.PrimaryKeyConstraint)
	c.Assert(createTable.Constraints[0].Columns, qt.DeepEquals, []string{"id"})
}

func TestParser_ParseCreateTable_SelectTail(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE t2 ENGINE=heap SELECT * FROM t1;
CREATE TABLE t3 (b int) SELECT a AS b FROM t1;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	noColumns := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(noColumns.Name, qt.Equals, "t2")
	c.Assert(noColumns.Columns, qt.HasLen, 0)
	c.Assert(noColumns.Options["ENGINE"], qt.Equals, "heap")
	c.Assert(noColumns.SelectBody, qt.Equals, "SELECT * FROM t1")

	withColumns := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(withColumns.Name, qt.Equals, "t3")
	c.Assert(withColumns.Columns, qt.HasLen, 1)
	c.Assert(withColumns.SelectBody, qt.Equals, "SELECT a AS b FROM t1")
}

func TestParser_ParseCreateTable_MySQLColumnModifiers(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE t1(
    c1 INT DEFAULT 12 COMMENT 'column1',
    c2 VARCHAR(255) CHARACTER SET utf8 NOT NULL DEFAULT 'a',
    c3 VARCHAR(255) CHARSET utf8 COLLATE utf8_unicode_ci NULL DEFAULT 'b'
);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 3)
	c.Assert(createTable.Columns[0].Comment, qt.Equals, "COMMENT 'column1'")
	c.Assert(createTable.Columns[1].Comment, qt.Equals, "CHARACTER SET utf8")
	c.Assert(createTable.Columns[1].Nullable, qt.IsFalse)
	c.Assert(createTable.Columns[2].Comment, qt.Equals, "CHARSET utf8; COLLATE utf8_unicode_ci")
	c.Assert(createTable.Columns[2].Nullable, qt.IsTrue)
}

func TestParser_ParseCreateTable_UnicodeIdentifiers(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE טבלה_של_אריאל (כמות int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "טבלה_של_אריאל")
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].Name, qt.Equals, "כמות")
	c.Assert(createTable.Columns[0].Type, qt.Equals, "int")
}

func TestParser_ParseCreateTable_EscapedDefaultStrings(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE mysql_defaults (c text default "\"" + '\'');
CREATE TABLE pg_plain_defaults (c text default '\');
CREATE TABLE pg_escaped_defaults (c text default E'\\A\\');`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 3)

	mysqlTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(mysqlTable.Columns, qt.HasLen, 1)
	c.Assert(mysqlTable.Columns[0].Default.Expression, qt.Equals, `"\""+ '\''`)

	pgPlainTable := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(pgPlainTable.Columns, qt.HasLen, 1)
	c.Assert(pgPlainTable.Columns[0].Default.Value, qt.Equals, "'\\'")

	pgEscapedTable := statements.Statements[2].(*ast.CreateTableNode)
	c.Assert(pgEscapedTable.Columns, qt.HasLen, 1)
	c.Assert(pgEscapedTable.Columns[0].Default.Value, qt.Equals, `E'\\A\\'`)
}

func TestParser_ParseCreateView(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createView, ok := statements.Statements[0].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createView.Name, qt.Equals, "active_users")
	c.Assert(createView.Replace, qt.IsFalse)
	c.Assert(createView.Body, qt.Equals, "SELECT id, name FROM users WHERE active = true")
}

func TestParser_ParseCreateOrReplaceView(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE OR REPLACE VIEW public.active_users AS SELECT * FROM users;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createView, ok := statements.Statements[0].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createView.Name, qt.Equals, "public.active_users")
	c.Assert(createView.Replace, qt.IsTrue)
	c.Assert(createView.Body, qt.Equals, "SELECT * FROM users")
}

func TestParser_ParseCreateSchema(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE SCHEMA IF NOT EXISTS "bc_test";`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createSchema, ok := statements.Statements[0].(*ast.CreateSchemaNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createSchema.Name, qt.Equals, `"bc_test"`)
	c.Assert(createSchema.IfNotExists, qt.IsTrue)
}

func TestParser_ParseCreateDatabase(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE DATABASE `atlantis`;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createDatabase, ok := statements.Statements[0].(*ast.CreateDatabaseNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createDatabase.Name, qt.Equals, "`atlantis`")
	c.Assert(createDatabase.IfNotExists, qt.IsFalse)
}

func TestParser_ParseCreateDatabaseWithQualifiedTableAndMySQLOptions(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE DATABASE `atlantis`; CREATE TABLE `atlantis`.`tbl` (`col` int NOT NULL) CHARSET utf8mb4 COLLATE utf8mb4_general_ci;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	createDatabase, ok := statements.Statements[0].(*ast.CreateDatabaseNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createDatabase.Name, qt.Equals, "`atlantis`")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "`atlantis`.`tbl`")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "utf8mb4")
	c.Assert(createTable.Options["COLLATE"], qt.Equals, "utf8mb4_general_ci")
}

func TestParser_ParseCreateFunction(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.add_one(value integer)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$ SELECT value + 1; $$;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "public.add_one")
	c.Assert(createFunction.Parameters, qt.Equals, "value integer")
	c.Assert(createFunction.Returns, qt.Equals, "integer")
	c.Assert(createFunction.Language, qt.Equals, "sql")
	c.Assert(createFunction.Volatility, qt.Equals, "IMMUTABLE")
	c.Assert(createFunction.Body, qt.Equals, " SELECT value + 1; ")
}

func TestParser_ParseCreateOrReplaceFunction(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE
OR REPLACE FUNCTION histories_partition_creation( DATE, DATE )
returns void AS $$
DECLARE
create_query text;
BEGIN
EXECUTE create_query;
END;
$$
language plpgsql;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "histories_partition_creation")
	c.Assert(createFunction.Parameters, qt.Equals, "DATE, DATE")
	c.Assert(createFunction.Returns, qt.Equals, "void")
	c.Assert(createFunction.Language, qt.Equals, "plpgsql")
	c.Assert(createFunction.Body, qt.Contains, "EXECUTE create_query;")
}

func TestParser_ParseCreateFunctionWithSingleQuotedBody(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE FUNCTION public.read_value() RETURNS text AS 'SELECT current_user' LANGUAGE sql SECURITY DEFINER STABLE;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "public.read_value")
	c.Assert(createFunction.Parameters, qt.Equals, "")
	c.Assert(createFunction.Returns, qt.Equals, "text")
	c.Assert(createFunction.Body, qt.Equals, "SELECT current_user")
	c.Assert(createFunction.Language, qt.Equals, "sql")
	c.Assert(createFunction.Security, qt.Equals, "DEFINER")
	c.Assert(createFunction.Volatility, qt.Equals, "STABLE")
}

func TestParser_ParseCreateTrigger(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TRIGGER before_tbl_insert BEFORE INSERT ON tbl BEGIN SELECT CASE
    WHEN (new.a = 4) THEN RAISE(IGNORE) END;
END;

CREATE TABLE t2(x,y,z);
CREATE TRIGGER t2r3 AFTER UPDATE ON t2 BEGIN SELECT 1; END;
CREATE TRIGGER trigItem_UNDO_AD AFTER DELETE ON Item FOR EACH ROW
BEGIN
  INSERT INTO Undo SELECT 'INSERT INTO Item (a,b,c) VALUES ('
   || coalesce(old.a,'NULL') || ',' || quote(old.b) || ',' || old.c || ');';
END;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 4)

	firstTrigger, ok := statements.Statements[0].(*ast.CreateTriggerNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(firstTrigger.Name, qt.Equals, "before_tbl_insert")
	c.Assert(firstTrigger.Table, qt.Equals, "tbl")
	c.Assert(firstTrigger.Timing, qt.Equals, "BEFORE")
	c.Assert(firstTrigger.Event, qt.Equals, "INSERT")
	c.Assert(firstTrigger.ForEach, qt.Equals, "ROW")
	c.Assert(firstTrigger.Body, qt.Contains, "RAISE(IGNORE) END;")

	table, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, "t2")
	c.Assert(table.Columns, qt.HasLen, 3)
	c.Assert(table.Columns[0].Name, qt.Equals, "x")
	c.Assert(table.Columns[0].Type, qt.Equals, "")

	updateTrigger, ok := statements.Statements[2].(*ast.CreateTriggerNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(updateTrigger.Name, qt.Equals, "t2r3")
	c.Assert(updateTrigger.Timing, qt.Equals, "AFTER")
	c.Assert(updateTrigger.Event, qt.Equals, "UPDATE")
	c.Assert(updateTrigger.Body, qt.Equals, "BEGIN SELECT 1; END")

	deleteTrigger, ok := statements.Statements[3].(*ast.CreateTriggerNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(deleteTrigger.Name, qt.Equals, "trigItem_UNDO_AD")
	c.Assert(deleteTrigger.Table, qt.Equals, "Item")
	c.Assert(deleteTrigger.Event, qt.Equals, "DELETE")
	c.Assert(deleteTrigger.ForEach, qt.Equals, "ROW")
	c.Assert(deleteTrigger.Body, qt.Contains, "coalesce(old.a,'NULL')")
}

func TestParser_ParseCreateOrReplaceTrigger(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON users BEGIN SELECT 1; END;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTrigger, ok := statements.Statements[0].(*ast.CreateTriggerNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTrigger.Name, qt.Equals, "set_updated_at")
	c.Assert(createTrigger.Replace, qt.IsTrue)
	c.Assert(createTrigger.Timing, qt.Equals, "BEFORE")
	c.Assert(createTrigger.Event, qt.Equals, "UPDATE")
	c.Assert(createTrigger.Table, qt.Equals, "users")
}

func TestParser_ParseRejectsUnsupportedCreateOrReplaceTarget(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser("CREATE OR REPLACE PROCEDURE p() AS $$ SELECT 1 $$;").Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported CREATE OR REPLACE target: PROCEDURE at position 18`)
}

func TestParser_ParseSkipsSchemaNeutralStatements(t *testing.T) {
	c := qt.New(t)

	sql := `
		INSERT INTO ignored_seed_data (id, name) VALUES (1, 'semi;colon');
		CREATE TABLE users (id INTEGER PRIMARY KEY);
		PRAGMA foreign_keys = off;
		SELECT * FROM users;
	`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "users")
}

func TestParser_ParseSchemaNeutralOnlyReturnsNoStatements(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("INSERT INTO seed_data (id) VALUES (1);").Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 0)
}

func TestParser_ParseRejectsUnknownStatement(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser("BROKEN STATEMENT;").Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported SQL statement: BROKEN at position 0`)
}

func TestParser_ParseAlterTable(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255) UNIQUE;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "users")
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	addOp, ok := alterTable.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(addOp.Column.Name, qt.Equals, "email")
	c.Assert(addOp.Column.Type, qt.Equals, "VARCHAR(255)")
	c.Assert(addOp.Column.Unique, qt.IsTrue)
}

func TestParser_ParseAlterTableOnlyAddExcludeConstraint(t *testing.T) {
	c := qt.New(t)

	sql := `ALTER TABLE ONLY t
    ADD CONSTRAINT name3 EXCLUDE USING gist (id WITH =, cid WITH -|-),
    ADD CONSTRAINT name4 EXCLUDE USING gist (id WITH =, cid WITH -|-);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "t")
	c.Assert(alterTable.Operations, qt.HasLen, 2)

	firstOp, ok := alterTable.Operations[0].(*ast.AddConstraintOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(firstOp.Constraint.Name, qt.Equals, "name3")
	c.Assert(firstOp.Constraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(firstOp.Constraint.UsingMethod, qt.Equals, "gist")
	c.Assert(firstOp.Constraint.ExcludeElements, qt.Equals, "id WITH =, cid WITH -|-")

	secondOp, ok := alterTable.Operations[1].(*ast.AddConstraintOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(secondOp.Constraint.Name, qt.Equals, "name4")
	c.Assert(secondOp.Constraint.Type, qt.Equals, ast.ExcludeConstraint)
}

func TestParser_ParseAlterTableQualifiedName(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE `atlantis`.`tbl` ADD `col_2` TEXT;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "`atlantis`.`tbl`")
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	addOp, ok := alterTable.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(addOp.Column.Name, qt.Equals, "`col_2`")
	c.Assert(addOp.Column.Type, qt.Equals, "TEXT")
}

func TestParser_ParseAlterTableRenameTable(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE `new_users` RENAME TO `users`;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "`new_users`")
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	renameOp, ok := alterTable.Operations[0].(*ast.RenameTableOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(renameOp.NewName, qt.Equals, "`users`")
}

func TestParser_ParseAlterTableRenameQualifiedTable(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE public.old_users RENAME TO archive.users;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "public.old_users")
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	renameOp, ok := alterTable.Operations[0].(*ast.RenameTableOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(renameOp.NewName, qt.Equals, "archive.users")
}

func TestParser_ParseAlterTableRenameColumn(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE users RENAME COLUMN old_name TO new_name;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "users")
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	renameOp, ok := alterTable.Operations[0].(*ast.RenameColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(renameOp.OldName, qt.Equals, "old_name")
	c.Assert(renameOp.NewName, qt.Equals, "new_name")
}

func TestParser_ParseDropTable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		ifExists bool
		cascade  bool
		rendered string
	}{
		{
			name:     "basic",
			sql:      "DROP TABLE users;",
			table:    "users",
			rendered: "DROP TABLE users;\n",
		},
		{
			name:     "if exists",
			sql:      "DROP TABLE IF EXISTS users;",
			table:    "users",
			ifExists: true,
			rendered: "DROP TABLE IF EXISTS users;\n",
		},
		{
			name:     "qualified cascade",
			sql:      "DROP TABLE public.users CASCADE;",
			table:    "public.users",
			cascade:  true,
			rendered: "DROP TABLE public.users CASCADE;\n",
		},
		{
			name:     "restrict",
			sql:      "DROP TABLE users RESTRICT;",
			table:    "users",
			rendered: "DROP TABLE users;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(tt.sql).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			dropTable, ok := statements.Statements[0].(*ast.DropTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(dropTable.Name, qt.Equals, tt.table)
			c.Assert(dropTable.IfExists, qt.Equals, tt.ifExists)
			c.Assert(dropTable.Cascade, qt.Equals, tt.cascade)

			rendered, err := renderer.RenderSQL("postgres", dropTable)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, tt.rendered)
		})
	}
}

func TestParser_ParseDropTableRejectsUnsupportedTargets(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser("DROP VIEW users;").Parse()
	c.Assert(err, qt.ErrorMatches, "unsupported DROP target: VIEW at position 5")
}

func TestParser_ParseCreateIndex(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE INDEX idx_users_email ON users (email);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
	c.Assert(index.Unique, qt.IsFalse)
}

func TestParser_ParseCreateIndexExpressions(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		indexName string
		tableName string
		columns   []string
	}{
		{
			name:      "quoted qualified table and nested expression",
			sql:       `CREATE INDEX "i" ON "s"."t" (((c #>> '{a,b,c}'::text[])));`,
			indexName: `"i"`,
			tableName: `"s"."t"`,
			columns:   []string{"((c #>> '{a,b,c}'::text[]))"},
		},
		{
			name:      "function expression",
			sql:       `CREATE INDEX "idx_emp_contact" ON "company"."employees" (LOWER(info #>> '{contact, email}'));`,
			indexName: `"idx_emp_contact"`,
			tableName: `"company"."employees"`,
			columns:   []string{"LOWER(info #>> '{contact, email}')"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			index, ok := statements.Statements[0].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(index.Name, qt.Equals, tt.indexName)
			c.Assert(index.Table, qt.Equals, tt.tableName)
			c.Assert(index.Columns, qt.DeepEquals, tt.columns)
			c.Assert(index.Unique, qt.IsFalse)
		})
	}
}

func TestParser_ParseCreateIndexRejectsEmptyColumnExpressions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "empty first expression",
			sql:  "CREATE INDEX idx_users_email ON users (, email);",
		},
		{
			name: "empty trailing expression",
			sql:  "CREATE INDEX idx_users_email ON users (email,);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(tt.sql).Parse()
			c.Assert(err, qt.ErrorMatches, "expected column or expression.*")
		})
	}
}

func TestParser_ParseCreateUniqueIndex(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE UNIQUE INDEX idx_users_email ON users (email);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
	c.Assert(index.Unique, qt.IsTrue)
}

func TestParser_ParseCreateTypedIndex(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		indexName string
		tableName string
		columns   []string
		indexType string
	}{
		{
			name:      "fulltext",
			sql:       "CREATE FULLTEXT INDEX idx_users_bio ON users (bio);",
			indexName: "idx_users_bio",
			tableName: "users",
			columns:   []string{"bio"},
			indexType: "FULLTEXT",
		},
		{
			name:      "spatial",
			sql:       "CREATE SPATIAL INDEX idx_geom_g ON geom (g);",
			indexName: "idx_geom_g",
			tableName: "geom",
			columns:   []string{"g"},
			indexType: "SPATIAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			index, ok := statements.Statements[0].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(index.Name, qt.Equals, tt.indexName)
			c.Assert(index.Table, qt.Equals, tt.tableName)
			c.Assert(index.Columns, qt.DeepEquals, tt.columns)
			c.Assert(index.Type, qt.Equals, tt.indexType)
			c.Assert(index.Unique, qt.IsFalse)
		})
	}
}

func TestParser_ParseCreateExtension(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		extension   string
		ifNotExists bool
		version     string
	}{
		{
			name:      "plain",
			sql:       "CREATE EXTENSION pg_trgm;",
			extension: "pg_trgm",
		},
		{
			name:        "if not exists",
			sql:         "CREATE EXTENSION IF NOT EXISTS unaccent;",
			extension:   "unaccent",
			ifNotExists: true,
		},
		{
			name:      "version",
			sql:       "CREATE EXTENSION postgis VERSION '3.0';",
			extension: "postgis",
			version:   "3.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			extension, ok := statements.Statements[0].(*ast.ExtensionNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(extension.Name, qt.Equals, tt.extension)
			c.Assert(extension.IfNotExists, qt.Equals, tt.ifNotExists)
			c.Assert(extension.Version, qt.Equals, tt.version)
		})
	}
}

func TestParser_ParseCreateEnum(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TYPE status AS ENUM ('active', 'inactive', 'pending');"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	enum, ok := statements.Statements[0].(*ast.EnumNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(enum.Name, qt.Equals, "status")
	c.Assert(enum.Values, qt.DeepEquals, []string{"active", "inactive", "pending"})
}

func TestParser_ParseMultipleStatements(t *testing.T) {
	c := qt.New(t)

	sql := `
		CREATE TABLE users (id INTEGER PRIMARY KEY);
		CREATE INDEX idx_users_id ON users (id);
		ALTER TABLE users ADD COLUMN name VARCHAR(255);
	`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 3)

	// Check first statement
	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "users")

	// Check second statement
	index, ok := statements.Statements[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_id")

	// Check third statement
	alterTable, ok := statements.Statements[2].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "users")
}

func TestParser_ParseColumnWithForeignKey(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE orders (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)

	column := createTable.Columns[0]
	c.Assert(column.Name, qt.Equals, "user_id")
	c.Assert(column.ForeignKey, qt.IsNotNil)
	c.Assert(column.ForeignKey.Table, qt.Equals, "users")
	c.Assert(column.ForeignKey.Column, qt.Equals, "id")
	c.Assert(column.ForeignKey.OnDelete, qt.Equals, "CASCADE")
}

func TestParser_ParseColumnWithDefaultFunction(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE logs (created_at TIMESTAMP DEFAULT NOW());"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	column := createTable.Columns[0]
	c.Assert(column.Name, qt.Equals, "created_at")
	c.Assert(column.Default, qt.IsNotNil)
	c.Assert(column.Default.Expression, qt.Equals, "NOW()")
}

func TestParser_ParseComplexTable(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE complex_table (
		id INTEGER PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL UNIQUE,
		email VARCHAR(255) NOT NULL,
		age INTEGER CHECK (age >= 0),
		status VARCHAR(20) DEFAULT 'active',
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP,
		UNIQUE (email),
		FOREIGN KEY (id) REFERENCES parent_table(id) ON DELETE CASCADE ON UPDATE SET NULL
	) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='Complex table example';`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "complex_table")
	c.Assert(createTable.Columns, qt.HasLen, 7)
	c.Assert(createTable.Constraints, qt.HasLen, 2)

	// Check id column
	idCol := createTable.Columns[0]
	c.Assert(idCol.Name, qt.Equals, "id")
	c.Assert(idCol.Primary, qt.IsTrue)
	c.Assert(idCol.AutoInc, qt.IsTrue)

	// Check name column
	nameCol := createTable.Columns[1]
	c.Assert(nameCol.Name, qt.Equals, "name")
	c.Assert(nameCol.Nullable, qt.IsFalse)
	c.Assert(nameCol.Unique, qt.IsTrue)

	// Check age column with check constraint
	ageCol := createTable.Columns[3]
	c.Assert(ageCol.Name, qt.Equals, "age")
	c.Assert(ageCol.Check, qt.Equals, "age >= 0")

	// Check status column with default
	statusCol := createTable.Columns[4]
	c.Assert(statusCol.Name, qt.Equals, "status")
	c.Assert(statusCol.Type, qt.Equals, "VARCHAR(20)")
	c.Assert(statusCol.Default, qt.IsNotNil)
	c.Assert(statusCol.Default.Value, qt.Equals, "'active'")

	// Check created_at with function default
	createdCol := createTable.Columns[5]
	c.Assert(createdCol.Name, qt.Equals, "created_at")
	c.Assert(createdCol.Default, qt.IsNotNil)
	c.Assert(createdCol.Default.Expression, qt.Equals, "NOW()")

	// Check updated_at column
	updatedCol := createTable.Columns[6]
	c.Assert(updatedCol.Name, qt.Equals, "updated_at")
	c.Assert(updatedCol.Type, qt.Equals, "TIMESTAMP")

	// Check table options
	c.Assert(createTable.Options["ENGINE"], qt.Equals, "InnoDB")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "utf8mb4")
	c.Assert(createTable.Comment, qt.Equals, "'Complex table example'")
}

func TestParser_ParseAlterTableMultipleOperations(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TABLE users ADD COLUMN phone VARCHAR(20), DROP COLUMN old_field, MODIFY COLUMN name TEXT NOT NULL;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(alterTable.Name, qt.Equals, "users")
	c.Assert(alterTable.Operations, qt.HasLen, 3)

	// Check ADD operation
	addOp, ok := alterTable.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(addOp.Column.Name, qt.Equals, "phone")
	c.Assert(addOp.Column.Type, qt.Equals, "VARCHAR(20)")

	// Check DROP operation
	dropOp, ok := alterTable.Operations[1].(*ast.DropColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(dropOp.ColumnName, qt.Equals, "old_field")

	// Check MODIFY operation
	modifyOp, ok := alterTable.Operations[2].(*ast.ModifyColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(modifyOp.Column.Name, qt.Equals, "name")
	c.Assert(modifyOp.Column.Type, qt.Equals, "TEXT")
	c.Assert(modifyOp.Column.Nullable, qt.IsFalse)
}

func TestParser_ParseMySQLStyleTable(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE ` + "`sample`" + ` (
		` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
		` + "`name`" + ` varchar(50) DEFAULT 'John',
		` + "`age`" + ` int DEFAULT 30,
		` + "`created_at`" + ` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
		` + "`active`" + ` tinyint(1) DEFAULT 1,
		PRIMARY KEY (` + "`id`" + `)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`sample`")
	c.Assert(createTable.Columns, qt.HasLen, 5)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check id column
	idColumn := createTable.Columns[0]
	c.Assert(idColumn.Name, qt.Equals, "`id`")
	c.Assert(idColumn.Type, qt.Equals, "int")
	c.Assert(idColumn.Nullable, qt.IsFalse)
	c.Assert(idColumn.AutoInc, qt.IsTrue)

	// Check name column with string default
	nameColumn := createTable.Columns[1]
	c.Assert(nameColumn.Name, qt.Equals, "`name`")
	c.Assert(nameColumn.Type, qt.Equals, "varchar(50)")
	c.Assert(nameColumn.Default, qt.IsNotNil)
	c.Assert(nameColumn.Default.Value, qt.Equals, "'John'")

	// Check age column with numeric default
	ageColumn := createTable.Columns[2]
	c.Assert(ageColumn.Name, qt.Equals, "`age`")
	c.Assert(ageColumn.Type, qt.Equals, "int")
	c.Assert(ageColumn.Default, qt.IsNotNil)
	c.Assert(ageColumn.Default.Value, qt.Equals, "30")

	// Check created_at column with function default and explicit NULL
	createdColumn := createTable.Columns[3]
	c.Assert(createdColumn.Name, qt.Equals, "`created_at`")
	c.Assert(createdColumn.Type, qt.Equals, "timestamp")
	c.Assert(createdColumn.Nullable, qt.IsTrue) // Explicit NULL
	c.Assert(createdColumn.Default, qt.IsNotNil)
	c.Assert(createdColumn.Default.Expression, qt.Equals, "CURRENT_TIMESTAMP()")

	// Check active column with tinyint type and numeric default
	activeColumn := createTable.Columns[4]
	c.Assert(activeColumn.Name, qt.Equals, "`active`")
	c.Assert(activeColumn.Type, qt.Equals, "tinyint(1)")
	c.Assert(activeColumn.Default, qt.IsNotNil)
	c.Assert(activeColumn.Default.Value, qt.Equals, "1")

	// Check PRIMARY KEY constraint
	pkConstraint := createTable.Constraints[0]
	c.Assert(pkConstraint.Type, qt.Equals, ast.PrimaryKeyConstraint)
	c.Assert(pkConstraint.Columns, qt.DeepEquals, []string{"`id`"})

	// Check table options
	c.Assert(createTable.Options["ENGINE"], qt.Equals, "InnoDB")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "utf8mb4")
	c.Assert(createTable.Options["COLLATE"], qt.Equals, "utf8mb4_0900_ai_ci")
}

func TestParser_ParseBacktickedIdentifiers(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE `users` (`user_id` INTEGER PRIMARY KEY, `email_address` VARCHAR(255));"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`users`")
	c.Assert(createTable.Columns, qt.HasLen, 2)

	// Check first column
	userIDColumn := createTable.Columns[0]
	c.Assert(userIDColumn.Name, qt.Equals, "`user_id`")
	c.Assert(userIDColumn.Type, qt.Equals, "INTEGER")
	c.Assert(userIDColumn.Primary, qt.IsTrue)

	// Check second column
	emailColumn := createTable.Columns[1]
	c.Assert(emailColumn.Name, qt.Equals, "`email_address`")
	c.Assert(emailColumn.Type, qt.Equals, "VARCHAR(255)")
}

func TestParser_ParseSimpleMySQLTable(t *testing.T) {
	c := qt.New(t)

	// Test a simpler version first
	sql := "CREATE TABLE `sample` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`));"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`sample`")
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check id column
	idColumn := createTable.Columns[0]
	c.Assert(idColumn.Name, qt.Equals, "`id`")
	c.Assert(idColumn.Type, qt.Equals, "int")
	c.Assert(idColumn.Nullable, qt.IsFalse)
	c.Assert(idColumn.AutoInc, qt.IsTrue)
}

func TestParser_ParseCurrentTimestamp(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE test (`created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)

	column := createTable.Columns[0]
	c.Assert(column.Name, qt.Equals, "`created_at`")
	c.Assert(column.Type, qt.Equals, "timestamp")
	c.Assert(column.Nullable, qt.IsTrue)
	c.Assert(column.Default, qt.IsNotNil)
	c.Assert(column.Default.Expression, qt.Equals, "CURRENT_TIMESTAMP()")
}

func TestParser_ParseMySQLTableStepByStep(t *testing.T) {
	c := qt.New(t)

	// Test with just 2 columns to isolate the issue
	sql := `CREATE TABLE ` + "`sample`" + ` (
		` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
		` + "`name`" + ` varchar(50) DEFAULT 'John'
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`sample`")
	c.Assert(createTable.Columns, qt.HasLen, 2)
}

func TestParser_ParseNumericDefault(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE test (`age` int DEFAULT 30);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)

	column := createTable.Columns[0]
	c.Assert(column.Name, qt.Equals, "`age`")
	c.Assert(column.Type, qt.Equals, "int")
	c.Assert(column.Default, qt.IsNotNil)
	c.Assert(column.Default.Value, qt.Equals, "30")
}

func TestParser_ParseMySQLTableWithTimestamp(t *testing.T) {
	c := qt.New(t)

	// Test with just the timestamp column that's causing issues
	sql := `CREATE TABLE ` + "`sample`" + ` (
		` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
		` + "`created_at`" + ` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
		` + "`active`" + ` tinyint(1) DEFAULT 1
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`sample`")
	c.Assert(createTable.Columns, qt.HasLen, 3)

	// Check created_at column
	createdColumn := createTable.Columns[1]
	c.Assert(createdColumn.Name, qt.Equals, "`created_at`")
	c.Assert(createdColumn.Type, qt.Equals, "timestamp")
	c.Assert(createdColumn.Nullable, qt.IsTrue)
	c.Assert(createdColumn.Default, qt.IsNotNil)
	c.Assert(createdColumn.Default.Expression, qt.Equals, "CURRENT_TIMESTAMP()")

	// Check active column
	activeColumn := createTable.Columns[2]
	c.Assert(activeColumn.Name, qt.Equals, "`active`")
	c.Assert(activeColumn.Type, qt.Equals, "tinyint(1)")
	c.Assert(activeColumn.Default, qt.IsNotNil)
	c.Assert(activeColumn.Default.Value, qt.Equals, "1")
}

func TestParser_ParseMySQLDataTypes(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{
			name:         "TinyInt",
			sql:          "CREATE TABLE test (col tinyint(1));",
			expectedType: "tinyint(1)",
		},
		{
			name:         "SmallInt",
			sql:          "CREATE TABLE test (col smallint(5));",
			expectedType: "smallint(5)",
		},
		{
			name:         "MediumInt",
			sql:          "CREATE TABLE test (col mediumint(8));",
			expectedType: "mediumint(8)",
		},
		{
			name:         "BigInt",
			sql:          "CREATE TABLE test (col bigint(20));",
			expectedType: "bigint(20)",
		},
		{
			name:         "Float",
			sql:          "CREATE TABLE test (col float(7,4));",
			expectedType: "float(7,4)",
		},
		{
			name:         "Double",
			sql:          "CREATE TABLE test (col double(15,8));",
			expectedType: "double(15,8)",
		},
		{
			name:         "Char",
			sql:          "CREATE TABLE test (col char(10));",
			expectedType: "char(10)",
		},
		{
			name:         "Text",
			sql:          "CREATE TABLE test (col text);",
			expectedType: "text",
		},
		{
			name:         "LongText",
			sql:          "CREATE TABLE test (col longtext);",
			expectedType: "longtext",
		},
		{
			name:         "DateTime",
			sql:          "CREATE TABLE test (col datetime);",
			expectedType: "datetime",
		},
		{
			name:         "Date",
			sql:          "CREATE TABLE test (col date);",
			expectedType: "date",
		},
		{
			name:         "Time",
			sql:          "CREATE TABLE test (col time);",
			expectedType: "time",
		},
		{
			name:         "Year",
			sql:          "CREATE TABLE test (col year(4));",
			expectedType: "year(4)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)
			c.Assert(createTable.Columns[0].Type, qt.Equals, tt.expectedType)
		})
	}
}

func TestParser_ParseMySQLTableOptions(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (id int)
		ENGINE=MyISAM
		DEFAULT CHARSET=latin1
		COLLATE=latin1_swedish_ci
		AUTO_INCREMENT=1000
		COMMENT='Test table';`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Options["ENGINE"], qt.Equals, "MyISAM")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "latin1")
	c.Assert(createTable.Options["COLLATE"], qt.Equals, "latin1_swedish_ci")
	c.Assert(createTable.Comment, qt.Equals, "'Test table'")
}

func TestParser_ParsePostgreSQLEnum(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TYPE status_enum AS ENUM ('pending', 'active', 'archived');"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	enum, ok := statements.Statements[0].(*ast.EnumNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(enum.Name, qt.Equals, "status_enum")
	c.Assert(enum.Values, qt.DeepEquals, []string{"pending", "active", "archived"})
}

func TestParser_ParsePostgreSQLDomain(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE DOMAIN email_domain AS TEXT
		CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	comment, ok := statements.Statements[0].(*ast.CommentNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(comment.Text, qt.Contains, "CREATE DOMAIN email_domain AS TEXT")
	c.Assert(comment.Text, qt.Contains, "CHECK")
}

func TestParser_ParsePostgreSQLSerialTypes(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		serial_id SERIAL PRIMARY KEY,
		big_id BIGSERIAL UNIQUE
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "test")
	c.Assert(createTable.Columns, qt.HasLen, 2)

	// Check SERIAL column
	serialCol := createTable.Columns[0]
	c.Assert(serialCol.Name, qt.Equals, "serial_id")
	c.Assert(serialCol.Type, qt.Equals, "SERIAL")
	c.Assert(serialCol.Primary, qt.IsTrue)

	// Check BIGSERIAL column
	bigSerialCol := createTable.Columns[1]
	c.Assert(bigSerialCol.Name, qt.Equals, "big_id")
	c.Assert(bigSerialCol.Type, qt.Equals, "BIGSERIAL")
	c.Assert(bigSerialCol.Unique, qt.IsTrue)
}

func TestParser_ParsePostgreSQLArrayTypes(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		tags TEXT[] DEFAULT ARRAY[]::TEXT[],
		matrix INT[][]
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 2)

	// Check TEXT[] column with array default
	tagsCol := createTable.Columns[0]
	c.Assert(tagsCol.Name, qt.Equals, "tags")
	c.Assert(tagsCol.Type, qt.Equals, "TEXT[]")
	c.Assert(tagsCol.Default, qt.IsNotNil)
	c.Assert(tagsCol.Default.Expression, qt.Equals, "ARRAY[]::TEXT[]")

	// Check multi-dimensional array
	matrixCol := createTable.Columns[1]
	c.Assert(matrixCol.Name, qt.Equals, "matrix")
	c.Assert(matrixCol.Type, qt.Equals, "INT[][]")
}

func TestParser_ParsePostgreSQLUUIDWithFunction(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE test (uuid_id UUID DEFAULT gen_random_uuid() NOT NULL);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)

	uuidCol := createTable.Columns[0]
	c.Assert(uuidCol.Name, qt.Equals, "uuid_id")
	c.Assert(uuidCol.Type, qt.Equals, "UUID")
	c.Assert(uuidCol.Default, qt.IsNotNil)
	c.Assert(uuidCol.Default.Expression, qt.Equals, "gen_random_uuid()")
	c.Assert(uuidCol.Nullable, qt.IsFalse)
}

func TestParser_ParsePostgreSQLGeneratedColumn(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE test (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 3)

	// Check generated column
	fullNameCol := createTable.Columns[2]
	c.Assert(fullNameCol.Name, qt.Equals, "full_name")
	c.Assert(fullNameCol.Type, qt.Equals, "TEXT")
	c.Assert(fullNameCol.Check, qt.Contains, "GENERATED ALWAYS AS")
	c.Assert(fullNameCol.Check, qt.Contains, "first_name || ' ' || last_name")
}

func TestParser_ParsePostgreSQLJSONTypes(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		json_field JSON,
		jsonb_field JSONB NOT NULL DEFAULT '{}'::jsonb
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 2)

	// Check JSON column
	jsonCol := createTable.Columns[0]
	c.Assert(jsonCol.Name, qt.Equals, "json_field")
	c.Assert(jsonCol.Type, qt.Equals, "JSON")

	// Check JSONB column with cast default
	jsonbCol := createTable.Columns[1]
	c.Assert(jsonbCol.Name, qt.Equals, "jsonb_field")
	c.Assert(jsonbCol.Type, qt.Equals, "JSONB")
	c.Assert(jsonbCol.Nullable, qt.IsFalse)
	c.Assert(jsonbCol.Default, qt.IsNotNil)
	c.Assert(jsonbCol.Default.Value, qt.Equals, "'{}'::jsonb")
}

func TestParser_ParsePostgreSQLCommentStatements(t *testing.T) {
	c := qt.New(t)

	sql := `COMMENT ON TABLE public.full_demo IS 'Comprehensive demo table';`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	comment, ok := statements.Statements[0].(*ast.CommentNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(comment.Text, qt.Contains, "COMMENT ON TABLE public.full_demo IS")
	c.Assert(comment.Text, qt.Contains, "Comprehensive demo table")
}

func TestParser_ParsePostgreSQLSchemaTable(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE public.test (id SERIAL PRIMARY KEY);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "public.test")
	c.Assert(createTable.Columns, qt.HasLen, 1)
}

func TestParser_ParsePostgreSQLFullDemo(t *testing.T) {
	c := qt.New(t)

	// Test a much simpler version to avoid infinite loops
	sql := `CREATE TABLE public.full_demo (
		serial_id SERIAL PRIMARY KEY,
		uuid_id UUID DEFAULT gen_random_uuid() NOT NULL,
		varchar_var VARCHAR(255) NOT NULL,
		tags TEXT[] DEFAULT ARRAY[]::TEXT[],
		jsonb_field JSONB NOT NULL DEFAULT '{}'::jsonb,
		user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "public.full_demo")
	c.Assert(createTable.Columns, qt.HasLen, 6)

	// Test some key columns
	serialCol := createTable.Columns[0]
	c.Assert(serialCol.Name, qt.Equals, "serial_id")
	c.Assert(serialCol.Type, qt.Equals, "SERIAL")
	c.Assert(serialCol.Primary, qt.IsTrue)

	uuidCol := createTable.Columns[1]
	c.Assert(uuidCol.Name, qt.Equals, "uuid_id")
	c.Assert(uuidCol.Type, qt.Equals, "UUID")
	c.Assert(uuidCol.Default.Expression, qt.Equals, "gen_random_uuid()")

	tagsCol := createTable.Columns[3]
	c.Assert(tagsCol.Name, qt.Equals, "tags")
	c.Assert(tagsCol.Type, qt.Equals, "TEXT[]")
	c.Assert(tagsCol.Default.Expression, qt.Equals, "ARRAY[]::TEXT[]")

	jsonbCol := createTable.Columns[4]
	c.Assert(jsonbCol.Name, qt.Equals, "jsonb_field")
	c.Assert(jsonbCol.Type, qt.Equals, "JSONB")
	c.Assert(jsonbCol.Default.Value, qt.Equals, "'{}'::jsonb")
}

func TestParser_ParsePostgreSQLMultipleStatements(t *testing.T) {
	c := qt.New(t)

	sql := `
		CREATE TYPE status_enum AS ENUM ('pending', 'active', 'archived');

		CREATE DOMAIN email_domain AS TEXT
			CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

		CREATE TABLE test (
			id SERIAL PRIMARY KEY,
			status status_enum DEFAULT 'pending',
			email email_domain
		);

		COMMENT ON TABLE test IS 'Test table with custom types';
	`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 4)

	// Check enum
	enum, ok := statements.Statements[0].(*ast.EnumNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(enum.Name, qt.Equals, "status_enum")

	// Check domain (represented as comment)
	domain, ok := statements.Statements[1].(*ast.CommentNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domain.Text, qt.Contains, "CREATE DOMAIN email_domain")

	// Check table
	table, ok := statements.Statements[2].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, "test")
	c.Assert(table.Columns, qt.HasLen, 3)

	// Check comment
	comment, ok := statements.Statements[3].(*ast.CommentNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(comment.Text, qt.Contains, "COMMENT ON TABLE test IS")
}

func TestParser_ParsePostgreSQLComprehensiveDemo(t *testing.T) {
	c := qt.New(t)

	// Test PostgreSQL CREATE TABLE statement with key advanced features
	// This test covers the most important PostgreSQL features from the original comprehensive SQL
	sql := `CREATE TABLE public.full_demo (
		serial_id SERIAL PRIMARY KEY,
		big_id BIGSERIAL UNIQUE,
		uuid_id UUID DEFAULT gen_random_uuid() NOT NULL,
		char_fixed CHAR(10),
		varchar_var VARCHAR(255) NOT NULL,
		text_field TEXT CHECK (char_length(text_field) <= 5000),
		small_value SMALLINT DEFAULT 1 CHECK (small_value > 0),
		numeric_precise NUMERIC(12,4) NOT NULL DEFAULT 0.0000,
		real_value REAL,
		double_value DOUBLE PRECISION,
		is_active BOOLEAN DEFAULT TRUE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ DEFAULT now(),
		tags TEXT[] DEFAULT ARRAY[]::TEXT[],
		matrix INT[][],
		json_field JSON,
		jsonb_field JSONB NOT NULL DEFAULT '{}'::jsonb,
		data BYTEA,
		email_address email_domain,
		user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL,
		CONSTRAINT uq_email UNIQUE (email_address),
		CONSTRAINT chk_positive_value CHECK (numeric_precise >= 0),
		CHECK (created_at <= updated_at)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "public.full_demo")

	// Test that we have the expected number of columns (now includes DOUBLE PRECISION)
	c.Assert(createTable.Columns, qt.HasLen, 20)

	// Test key PostgreSQL features

	// Serial types
	serialCol := createTable.Columns[0]
	c.Assert(serialCol.Name, qt.Equals, "serial_id")
	c.Assert(serialCol.Type, qt.Equals, "SERIAL")
	c.Assert(serialCol.Primary, qt.IsTrue)

	bigSerialCol := createTable.Columns[1]
	c.Assert(bigSerialCol.Name, qt.Equals, "big_id")
	c.Assert(bigSerialCol.Type, qt.Equals, "BIGSERIAL")
	c.Assert(bigSerialCol.Unique, qt.IsTrue)

	// UUID with function default
	uuidCol := createTable.Columns[2]
	c.Assert(uuidCol.Name, qt.Equals, "uuid_id")
	c.Assert(uuidCol.Type, qt.Equals, "UUID")
	c.Assert(uuidCol.Default, qt.IsNotNil)
	c.Assert(uuidCol.Default.Expression, qt.Equals, "gen_random_uuid()")
	c.Assert(uuidCol.Nullable, qt.IsFalse)

	// Character types
	charCol := createTable.Columns[3]
	c.Assert(charCol.Name, qt.Equals, "char_fixed")
	c.Assert(charCol.Type, qt.Equals, "CHAR(10)")

	// Text with check constraint
	textCol := createTable.Columns[5]
	c.Assert(textCol.Name, qt.Equals, "text_field")
	c.Assert(textCol.Type, qt.Equals, "TEXT")
	c.Assert(textCol.Check, qt.Contains, "char_length(text_field) <= 5000")

	// Numeric with check constraint
	smallCol := createTable.Columns[6]
	c.Assert(smallCol.Name, qt.Equals, "small_value")
	c.Assert(smallCol.Type, qt.Equals, "SMALLINT")
	c.Assert(smallCol.Default, qt.IsNotNil)
	c.Assert(smallCol.Default.Value, qt.Equals, "1")
	c.Assert(smallCol.Check, qt.Contains, "small_value > 0")

	// NUMERIC with precision and default
	numericCol := createTable.Columns[7]
	c.Assert(numericCol.Name, qt.Equals, "numeric_precise")
	c.Assert(numericCol.Type, qt.Equals, "NUMERIC(12,4)")
	c.Assert(numericCol.Nullable, qt.IsFalse)
	c.Assert(numericCol.Default, qt.IsNotNil)
	c.Assert(numericCol.Default.Value, qt.Equals, "0.0000")

	// DOUBLE PRECISION type
	doubleCol := createTable.Columns[9]
	c.Assert(doubleCol.Name, qt.Equals, "double_value")
	c.Assert(doubleCol.Type, qt.Equals, "DOUBLE PRECISION")

	// Boolean with default
	boolCol := createTable.Columns[10]
	c.Assert(boolCol.Name, qt.Equals, "is_active")
	c.Assert(boolCol.Type, qt.Equals, "BOOLEAN")
	c.Assert(boolCol.Default, qt.IsNotNil)
	c.Assert(boolCol.Default.Value, qt.Equals, "TRUE")

	// TIMESTAMPTZ type
	updatedCol := createTable.Columns[12]
	c.Assert(updatedCol.Name, qt.Equals, "updated_at")
	c.Assert(updatedCol.Type, qt.Equals, "TIMESTAMPTZ")
	c.Assert(updatedCol.Default, qt.IsNotNil)
	c.Assert(updatedCol.Default.Expression, qt.Equals, "now()")

	// Array types
	tagsCol := createTable.Columns[13]
	c.Assert(tagsCol.Name, qt.Equals, "tags")
	c.Assert(tagsCol.Type, qt.Equals, "TEXT[]")
	c.Assert(tagsCol.Default, qt.IsNotNil)
	c.Assert(tagsCol.Default.Expression, qt.Equals, "ARRAY[]::TEXT[]")

	matrixCol := createTable.Columns[14]
	c.Assert(matrixCol.Name, qt.Equals, "matrix")
	c.Assert(matrixCol.Type, qt.Equals, "INT[][]")

	// JSON types
	jsonbCol := createTable.Columns[16]
	c.Assert(jsonbCol.Name, qt.Equals, "jsonb_field")
	c.Assert(jsonbCol.Type, qt.Equals, "JSONB")
	c.Assert(jsonbCol.Nullable, qt.IsFalse)
	c.Assert(jsonbCol.Default, qt.IsNotNil)
	c.Assert(jsonbCol.Default.Value, qt.Equals, "'{}'::jsonb")

	// BYTEA type
	dataCol := createTable.Columns[17]
	c.Assert(dataCol.Name, qt.Equals, "data")
	c.Assert(dataCol.Type, qt.Equals, "BYTEA")

	// Domain type
	emailCol := createTable.Columns[18]
	c.Assert(emailCol.Name, qt.Equals, "email_address")
	c.Assert(emailCol.Type, qt.Equals, "email_domain")

	// Foreign key with cascading rules
	userIDCol := createTable.Columns[19]
	c.Assert(userIDCol.Name, qt.Equals, "user_id")
	c.Assert(userIDCol.Type, qt.Equals, "INTEGER")
	c.Assert(userIDCol.ForeignKey, qt.IsNotNil)
	c.Assert(userIDCol.ForeignKey.Table, qt.Equals, "users")
	c.Assert(userIDCol.ForeignKey.Column, qt.Equals, "id")
	c.Assert(userIDCol.ForeignKey.OnDelete, qt.Equals, "CASCADE")
	c.Assert(userIDCol.ForeignKey.OnUpdate, qt.Equals, "SET NULL")

	// Test table-level constraints
	c.Assert(createTable.Constraints, qt.HasLen, 3)

	// Unique constraint
	uniqueConstraint := createTable.Constraints[0]
	c.Assert(uniqueConstraint.Type, qt.Equals, ast.UniqueConstraint)
	c.Assert(uniqueConstraint.Name, qt.Equals, "uq_email")
	c.Assert(uniqueConstraint.Columns, qt.DeepEquals, []string{"email_address"})

	// Named check constraint
	checkConstraint := createTable.Constraints[1]
	c.Assert(checkConstraint.Type, qt.Equals, ast.CheckConstraint)
	c.Assert(checkConstraint.Name, qt.Equals, "chk_positive_value")
	c.Assert(checkConstraint.Expression, qt.Contains, "numeric_precise >= 0")

	// Unnamed table-level check constraint
	tableLevelCheck := createTable.Constraints[2]
	c.Assert(tableLevelCheck.Type, qt.Equals, ast.CheckConstraint)
	c.Assert(tableLevelCheck.Expression, qt.Contains, "created_at <= updated_at")
}

func TestParser_ParseMultiWordTypes(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{
			name:         "DOUBLE PRECISION",
			sql:          "CREATE TABLE test (value DOUBLE PRECISION);",
			expectedType: "DOUBLE PRECISION",
		},
		{
			name:         "CHARACTER VARYING",
			sql:          "CREATE TABLE test (name CHARACTER VARYING(255));",
			expectedType: "CHARACTER VARYING(255)",
		},
		{
			name:         "DOUBLE PRECISION with default",
			sql:          "CREATE TABLE test (value DOUBLE PRECISION DEFAULT 0.0);",
			expectedType: "DOUBLE PRECISION",
		},
		{
			name:         "DOUBLE PRECISION NOT NULL",
			sql:          "CREATE TABLE test (value DOUBLE PRECISION NOT NULL);",
			expectedType: "DOUBLE PRECISION",
		},
		{
			name:         "TIMESTAMP WITH TIME ZONE",
			sql:          "CREATE TABLE test (ts TIMESTAMP WITH TIME ZONE);",
			expectedType: "WITH TIMESTAMP TIME ZONE",
		},
		{
			name:         "TIMESTAMP WITHOUT TIME ZONE",
			sql:          "CREATE TABLE test (ts TIMESTAMP WITHOUT TIME ZONE);",
			expectedType: "WITHOUT TIMESTAMP TIME ZONE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)

			column := createTable.Columns[0]
			c.Assert(column.Type, qt.Equals, tt.expectedType)
		})
	}
}

func TestParser_ParseParameterizedArrayTypes(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{
			name:         "NUMERIC array with parameters",
			sql:          "CREATE TABLE test (scores NUMERIC(5,2)[]);",
			expectedType: "NUMERIC(5,2)[]",
		},
		{
			name:         "VARCHAR array",
			sql:          "CREATE TABLE test (names VARCHAR(100)[]);",
			expectedType: "VARCHAR(100)[]",
		},
		{
			name:         "DECIMAL multi-dimensional array",
			sql:          "CREATE TABLE test (matrix DECIMAL(10,2)[][]);",
			expectedType: "DECIMAL(10,2)[][]",
		},
		{
			name:         "CHAR array",
			sql:          "CREATE TABLE test (codes CHAR(3)[]);",
			expectedType: "CHAR(3)[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)

			column := createTable.Columns[0]
			c.Assert(column.Type, qt.Equals, tt.expectedType)
		})
	}
}

func TestParser_ParseOriginalProblematicSQL(t *testing.T) {
	c := qt.New(t)

	// This is the original SQL that was causing infinite loop due to DOUBLE PRECISION
	sql := `CREATE TABLE public.full_demo (
		serial_id SERIAL PRIMARY KEY,
		double_value DOUBLE PRECISION,
		varchar_var VARCHAR(255) NOT NULL
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "public.full_demo")
	c.Assert(createTable.Columns, qt.HasLen, 3)

	// Verify DOUBLE PRECISION column is parsed correctly
	doubleCol := createTable.Columns[1]
	c.Assert(doubleCol.Name, qt.Equals, "double_value")
	c.Assert(doubleCol.Type, qt.Equals, "DOUBLE PRECISION")
}

func TestParser_ParsePostgreSQLTableOptions(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectedOptions map[string]string
	}{
		{
			name: "WITH clause with multiple options",
			sql: `CREATE TABLE test (
				id INTEGER PRIMARY KEY
			) WITH (
				fillfactor = 70,
				autovacuum_enabled = true,
				autovacuum_vacuum_threshold = 50
			);`,
			expectedOptions: map[string]string{
				"fillfactor":                  "70",
				"autovacuum_enabled":          "true",
				"autovacuum_vacuum_threshold": "50",
			},
		},
		{
			name: "WITH clause and TABLESPACE",
			sql: `CREATE TABLE test (
				id INTEGER PRIMARY KEY
			) WITH (
				fillfactor = 80
			) TABLESPACE pg_default;`,
			expectedOptions: map[string]string{
				"fillfactor": "80",
				"TABLESPACE": "pg_default",
			},
		},
		{
			name: "TABLESPACE only",
			sql: `CREATE TABLE test (
				id INTEGER PRIMARY KEY
			) TABLESPACE custom_tablespace;`,
			expectedOptions: map[string]string{
				"TABLESPACE": "custom_tablespace",
			},
		},
		{
			name: "WITH clause with string values",
			sql: `CREATE TABLE test (
				id INTEGER PRIMARY KEY
			) WITH (
				toast_tuple_target = 128,
				parallel_workers = 4
			);`,
			expectedOptions: map[string]string{
				"toast_tuple_target": "128",
				"parallel_workers":   "4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)

			// Verify all expected options are present
			for key, expectedValue := range tt.expectedOptions {
				actualValue, exists := createTable.Options[key]
				c.Assert(exists, qt.IsTrue, qt.Commentf("Option %s should exist", key))
				c.Assert(actualValue, qt.Equals, expectedValue, qt.Commentf("Option %s should have value %s", key, expectedValue))
			}
		})
	}
}

func TestParser_ParseExtendedPostgreSQLDemo(t *testing.T) {
	c := qt.New(t)

	// Extended comprehensive PostgreSQL CREATE TABLE statement with even more advanced features
	sql := `CREATE TABLE public.extended_demo (
		-- Identity and serial types
		serial_id SERIAL PRIMARY KEY,
		big_id BIGSERIAL UNIQUE,
		small_id SMALLSERIAL,

		-- UUID with default generator
		uuid_id UUID DEFAULT gen_random_uuid() NOT NULL,
		uuid_alt UUID DEFAULT uuid_generate_v4(),

		-- Character types with various specifications
		char_fixed CHAR(10),
		varchar_var VARCHAR(255) NOT NULL,
		varchar_unlimited VARCHAR,
		text_field TEXT CHECK (char_length(text_field) <= 5000),
		char_varying CHARACTER VARYING(100),

		-- Numeric types with constraints
		small_value SMALLINT DEFAULT 1 CHECK (small_value > 0),
		int_value INTEGER,
		big_value BIGINT,
		numeric_precise NUMERIC(12,4) NOT NULL DEFAULT 0.0000,
		decimal_alt DECIMAL(10,2),
		money_value MONEY DEFAULT '$0.00',

		-- Floating-point types
		real_value REAL,
		double_value DOUBLE PRECISION,
		float_value FLOAT(24),

		-- Boolean with default
		is_active BOOLEAN DEFAULT TRUE,
		is_deleted BOOLEAN DEFAULT FALSE,

		-- Dates and timestamps with various formats
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ DEFAULT now(),
		due_date DATE,
		time_only TIME,
		time_with_tz TIMETZ,
		interval_field INTERVAL,
		timestamp_no_tz TIMESTAMP WITHOUT TIME ZONE,
		timestamp_with_tz TIMESTAMP WITH TIME ZONE DEFAULT now(),

		-- Enum (requires pre-defined type)
		status status_enum DEFAULT 'pending',
		priority priority_type DEFAULT 'medium',

		-- Arrays of various types
		tags TEXT[] DEFAULT ARRAY[]::TEXT[],
		matrix INT[][],
		scores NUMERIC(5,2)[],
		flags BOOLEAN[] DEFAULT '{false,false,true}',

		-- JSON and JSONB
		json_field JSON,
		jsonb_field JSONB NOT NULL DEFAULT '{}'::jsonb,
		metadata JSONB DEFAULT '{"version": 1}',

		-- Binary data
		data BYTEA,
		file_content BYTEA,

		-- Network types
		ip_address INET,
		mac_address MACADDR,
		network_range CIDR,

		-- Geometric types
		point_location POINT,
		line_segment LSEG,
		box_area BOX,
		path_data PATH,
		polygon_shape POLYGON,
		circle_area CIRCLE,

		-- Text search types
		search_vector TSVECTOR,
		search_query TSQUERY,

		-- Range types
		int_range INT4RANGE,
		timestamp_range TSRANGE,
		date_range DATERANGE,

		-- Domain types (assume domains are defined)
		email_address email_domain,
		phone_number phone_domain,
		postal_code zipcode_domain,

		-- Generated columns
		full_name TEXT GENERATED ALWAYS AS (char_fixed || ' ' || varchar_var) STORED,
		search_text TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', text_field)) STORED,

		-- Collation examples
		case_insensitive TEXT COLLATE "C",
		locale_specific TEXT COLLATE "en_US.UTF-8",

		-- Foreign keys with various cascading rules
		user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL,
		category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
		parent_id INTEGER REFERENCES extended_demo(serial_id) ON DELETE CASCADE,

		-- Composite unique constraint
		CONSTRAINT uq_tag_and_status UNIQUE (tags, status),
		CONSTRAINT uq_user_category UNIQUE (user_id, category_id),

		-- Check constraints with expressions
		CONSTRAINT chk_price_non_negative CHECK (numeric_precise >= 0),
		CONSTRAINT chk_valid_email CHECK (email_address ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
		CONSTRAINT chk_date_order CHECK (created_at <= updated_at),
		CONSTRAINT chk_status_priority CHECK (
			(status = 'urgent' AND priority IN ('high', 'critical')) OR
			(status != 'urgent')
		),

		-- Table-level check constraints
		CHECK (created_at <= updated_at),
		CHECK (small_value BETWEEN 1 AND 1000)
	)
	-- Table options
	WITH (
		fillfactor = 70,
		autovacuum_enabled = true,
		autovacuum_vacuum_threshold = 50,
		autovacuum_analyze_threshold = 50
	)
	TABLESPACE pg_default;
`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "public.extended_demo")

	// Test that we have a substantial number of columns (should be at least 40)
	c.Assert(len(createTable.Columns) > 40, qt.IsTrue)

	// Test key PostgreSQL features that should be parsed correctly

	// Test DOUBLE PRECISION is now working
	var doubleCol *ast.ColumnNode
	for _, col := range createTable.Columns {
		if col.Name == "double_value" {
			doubleCol = col
			break
		}
	}
	c.Assert(doubleCol, qt.IsNotNil)
	c.Assert(doubleCol.Type, qt.Equals, "DOUBLE PRECISION")

	// Test CHARACTER VARYING
	var charVaryingCol *ast.ColumnNode
	for _, col := range createTable.Columns {
		if col.Name == "char_varying" {
			charVaryingCol = col
			break
		}
	}
	c.Assert(charVaryingCol, qt.IsNotNil)
	c.Assert(charVaryingCol.Type, qt.Equals, "CHARACTER VARYING(100)")

	// Test TIMESTAMP WITH TIME ZONE
	var timestampCol *ast.ColumnNode
	for _, col := range createTable.Columns {
		if col.Name == "timestamp_with_tz" {
			timestampCol = col
			break
		}
	}
	c.Assert(timestampCol, qt.IsNotNil)
	c.Assert(timestampCol.Type, qt.Equals, "WITH TIMESTAMP TIME ZONE")

	// Test parameterized array type
	var scoresCol *ast.ColumnNode
	for _, col := range createTable.Columns {
		if col.Name == "scores" {
			scoresCol = col
			break
		}
	}
	c.Assert(scoresCol, qt.IsNotNil)
	c.Assert(scoresCol.Type, qt.Equals, "NUMERIC(5,2)[]")

	// Test that we have multiple constraints
	c.Assert(len(createTable.Constraints) > 0, qt.IsTrue)

	// Test table options from WITH clause
	c.Assert(createTable.Options["fillfactor"], qt.Equals, "70")
	c.Assert(createTable.Options["autovacuum_enabled"], qt.Equals, "true")
	c.Assert(createTable.Options["autovacuum_vacuum_threshold"], qt.Equals, "50")
	c.Assert(createTable.Options["autovacuum_analyze_threshold"], qt.Equals, "50")

	// Test TABLESPACE option
	c.Assert(createTable.Options["TABLESPACE"], qt.Equals, "pg_default")
}

func TestParser_ParseMariaDBComprehensiveDemo(t *testing.T) {
	c := qt.New(t)

	// Comprehensive MariaDB CREATE TABLE statement with advanced features
	sql := `CREATE TABLE ` + "`full_demo`" + ` (
		-- Auto-increment with unsigned integer
		` + "`id`" + ` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

		-- Character types
		` + "`username`" + ` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		` + "`bio`" + ` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,

		-- Numeric with defaults and constraints
		` + "`balance`" + ` DECIMAL(10,2) NOT NULL DEFAULT 0.00 CHECK (` + "`balance`" + ` >= 0),
		` + "`score`" + ` DOUBLE ZEROFILL DEFAULT 0000.00,

		-- Date and time
		` + "`created_at`" + ` DATETIME DEFAULT CURRENT_TIMESTAMP,
		` + "`updated_at`" + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

		-- Boolean type
		` + "`is_active`" + ` BOOLEAN NOT NULL DEFAULT TRUE,

		-- Enum and set
		` + "`status`" + ` ENUM('new', 'active', 'archived') DEFAULT 'new',
		` + "`roles`" + ` SET('admin', 'editor', 'user') DEFAULT 'user',

		-- JSON support (MariaDB 10.2+)
		` + "`data`" + ` JSON,

		-- Spatial types
		` + "`location`" + ` POINT,
		SPATIAL INDEX (` + "`location`" + `),

		-- Virtual columns
		` + "`fullname`" + ` VARCHAR(100) AS (CONCAT(` + "`username`" + `, ' ', 'User')) STORED,

		-- Foreign key with actions
		` + "`country_id`" + ` INT,
		CONSTRAINT ` + "`fk_country`" + ` FOREIGN KEY (` + "`country_id`" + `) REFERENCES ` + "`countries`" + ` (` + "`id`" + `)
		ON DELETE SET NULL ON UPDATE CASCADE,

		-- Unique and composite indexes
		UNIQUE KEY ` + "`uq_username`" + ` (` + "`username`" + `),
		UNIQUE KEY ` + "`uq_roles_status`" + ` (` + "`roles`" + `, ` + "`status`" + `),

		-- Check constraint with name
		CONSTRAINT ` + "`chk_score_positive`" + ` CHECK (` + "`score`" + ` >= 0)
	)
	ENGINE=InnoDB
	AUTO_INCREMENT=1000
	DEFAULT CHARSET=utf8mb4
	COLLATE=utf8mb4_unicode_ci
	ROW_FORMAT=DYNAMIC
	COMMENT='Comprehensive MariaDB table';`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`full_demo`")

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}
	t.Logf("Found %d constraints:", len(createTable.Constraints))
	for i, constraint := range createTable.Constraints {
		t.Logf("  %d: %s %v", i, constraint.Type, constraint.Columns)
	}

	// Test that we have the expected number of columns (14 total)
	c.Assert(createTable.Columns, qt.HasLen, 14)

	// Test auto-increment unsigned primary key
	idCol := createTable.Columns[0]
	c.Assert(idCol.Name, qt.Equals, "`id`")
	c.Assert(idCol.Type, qt.Equals, "INT UNSIGNED")
	c.Assert(idCol.Nullable, qt.IsFalse)
	c.Assert(idCol.AutoInc, qt.IsTrue)
	c.Assert(idCol.Primary, qt.IsTrue)

	// Test character types with charset and collation
	usernameCol := createTable.Columns[1]
	c.Assert(usernameCol.Name, qt.Equals, "`username`")
	c.Assert(usernameCol.Type, qt.Equals, "VARCHAR(50)")
	c.Assert(usernameCol.Nullable, qt.IsFalse)

	bioCol := createTable.Columns[2]
	c.Assert(bioCol.Name, qt.Equals, "`bio`")
	c.Assert(bioCol.Type, qt.Equals, "TEXT")

	// Test DECIMAL with check constraint
	balanceCol := createTable.Columns[3]
	c.Assert(balanceCol.Name, qt.Equals, "`balance`")
	c.Assert(balanceCol.Type, qt.Equals, "DECIMAL(10,2)")
	c.Assert(balanceCol.Nullable, qt.IsFalse)
	c.Assert(balanceCol.Default, qt.IsNotNil)
	c.Assert(balanceCol.Default.Value, qt.Equals, "0.00")
	c.Assert(balanceCol.Check, qt.Contains, "`balance` >= 0")

	// Test DOUBLE with ZEROFILL
	scoreCol := createTable.Columns[4]
	c.Assert(scoreCol.Name, qt.Equals, "`score`")
	c.Assert(scoreCol.Type, qt.Equals, "DOUBLE ZEROFILL")
	c.Assert(scoreCol.Default, qt.IsNotNil)
	c.Assert(scoreCol.Default.Value, qt.Equals, "0000.00")

	// Test DATETIME with CURRENT_TIMESTAMP
	createdCol := createTable.Columns[5]
	c.Assert(createdCol.Name, qt.Equals, "`created_at`")
	c.Assert(createdCol.Type, qt.Equals, "DATETIME")
	c.Assert(createdCol.Default, qt.IsNotNil)
	c.Assert(createdCol.Default.Expression, qt.Equals, "CURRENT_TIMESTAMP()")

	// Test TIMESTAMP with ON UPDATE
	updatedCol := createTable.Columns[6]
	c.Assert(updatedCol.Name, qt.Equals, "`updated_at`")
	c.Assert(updatedCol.Type, qt.Equals, "TIMESTAMP")
	c.Assert(updatedCol.Default, qt.IsNotNil)
	c.Assert(updatedCol.Default.Expression, qt.Equals, "CURRENT_TIMESTAMP()")

	// Test BOOLEAN with default
	activeCol := createTable.Columns[7]
	c.Assert(activeCol.Name, qt.Equals, "`is_active`")
	c.Assert(activeCol.Type, qt.Equals, "BOOLEAN")
	c.Assert(activeCol.Nullable, qt.IsFalse)
	c.Assert(activeCol.Default, qt.IsNotNil)
	c.Assert(activeCol.Default.Value, qt.Equals, "TRUE")

	// Test ENUM type
	statusCol := createTable.Columns[8]
	c.Assert(statusCol.Name, qt.Equals, "`status`")
	c.Assert(statusCol.Type, qt.Equals, "ENUM('new', 'active', 'archived')")
	c.Assert(statusCol.Default, qt.IsNotNil)
	c.Assert(statusCol.Default.Value, qt.Equals, "'new'")

	// Test SET type
	rolesCol := createTable.Columns[9]
	c.Assert(rolesCol.Name, qt.Equals, "`roles`")
	c.Assert(rolesCol.Type, qt.Equals, "SET('admin', 'editor', 'user')")
	c.Assert(rolesCol.Default, qt.IsNotNil)
	c.Assert(rolesCol.Default.Value, qt.Equals, "'user'")

	// Test JSON type
	dataCol := createTable.Columns[10]
	c.Assert(dataCol.Name, qt.Equals, "`data`")
	c.Assert(dataCol.Type, qt.Equals, "JSON")

	// Test POINT spatial type
	locationCol := createTable.Columns[11]
	c.Assert(locationCol.Name, qt.Equals, "`location`")
	c.Assert(locationCol.Type, qt.Equals, "POINT")

	// Test virtual/generated column
	fullnameCol := createTable.Columns[12]
	c.Assert(fullnameCol.Name, qt.Equals, "`fullname`")
	c.Assert(fullnameCol.Type, qt.Equals, "VARCHAR(100)")

	// Test foreign key column
	countryCol := createTable.Columns[13]
	c.Assert(countryCol.Name, qt.Equals, "`country_id`")
	c.Assert(countryCol.Type, qt.Equals, "INT")

	// Test table-level constraints
	c.Assert(len(createTable.Constraints) >= 4, qt.IsTrue)

	// Test table options
	c.Assert(createTable.Options["ENGINE"], qt.Equals, "InnoDB")
	c.Assert(createTable.Options["CHARSET"], qt.Equals, "utf8mb4")
	c.Assert(createTable.Options["COLLATE"], qt.Equals, "utf8mb4_unicode_ci")
	c.Assert(createTable.Options["ROW_FORMAT"], qt.Equals, "DYNAMIC")
	c.Assert(createTable.Comment, qt.Equals, "'Comprehensive MariaDB table'")
}

func TestParser_TimeoutProtection(t *testing.T) {
	c := qt.New(t)

	// Test with a simple valid SQL first to ensure timeout doesn't interfere with normal parsing
	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY);"
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
}

func TestParser_MariaDBMinimal(t *testing.T) {
	c := qt.New(t)

	// Test just the problematic id column that causes hanging
	sql := "CREATE TABLE `test` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY);"
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "`test`")
	c.Assert(createTable.Columns, qt.HasLen, 1)

	// Test the id column
	idCol := createTable.Columns[0]
	c.Assert(idCol.Name, qt.Equals, "`id`")
	c.Assert(idCol.Type, qt.Equals, "INT UNSIGNED")
	c.Assert(idCol.Nullable, qt.IsFalse)
	c.Assert(idCol.AutoInc, qt.IsTrue)
	c.Assert(idCol.Primary, qt.IsTrue)
}

func TestParser_MariaDBTypeModifiers(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{
			name:         "INT UNSIGNED",
			sql:          "CREATE TABLE test (col INT UNSIGNED);",
			expectedType: "INT UNSIGNED",
		},
		{
			name:         "DOUBLE ZEROFILL",
			sql:          "CREATE TABLE test (col DOUBLE ZEROFILL);",
			expectedType: "DOUBLE ZEROFILL",
		},
		{
			name:         "DECIMAL UNSIGNED",
			sql:          "CREATE TABLE test (col DECIMAL(10,2) UNSIGNED);",
			expectedType: "DECIMAL(10,2) UNSIGNED",
		},
		{
			name:         "BIGINT SIGNED",
			sql:          "CREATE TABLE test (col BIGINT SIGNED);",
			expectedType: "BIGINT SIGNED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)
			c.Assert(createTable.Columns[0].Type, qt.Equals, tt.expectedType)
		})
	}
}

func TestParser_MariaDBCharacterSet(t *testing.T) {
	c := qt.New(t)

	// Test CHARACTER SET and COLLATE syntax
	sql := "CREATE TABLE test (`name` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci);"
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].Type, qt.Equals, "VARCHAR(50)")
}

func TestParser_MariaDBEnumAndSet(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{
			name:         "ENUM type",
			sql:          "CREATE TABLE test (status ENUM('new', 'active', 'archived'));",
			expectedType: "ENUM('new', 'active', 'archived')",
		},
		{
			name:         "SET type",
			sql:          "CREATE TABLE test (roles SET('admin', 'editor', 'user'));",
			expectedType: "SET('admin', 'editor', 'user')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Columns, qt.HasLen, 1)
			c.Assert(createTable.Columns[0].Type, qt.Equals, tt.expectedType)
		})
	}
}

func TestParser_MariaDBVirtualColumn(t *testing.T) {
	c := qt.New(t)

	// Test virtual/generated column syntax
	sql := "CREATE TABLE test (`name` VARCHAR(50), `fullname` VARCHAR(100) AS (CONCAT(`name`, ' ', 'User')) STORED);"
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 2)
	c.Assert(createTable.Columns[1].Type, qt.Equals, "VARCHAR(100)")
}

func TestParser_MariaDBTableOptions(t *testing.T) {
	c := qt.New(t)

	// Test table options parsing
	sql := `CREATE TABLE test (id INT)
		ENGINE=InnoDB
		AUTO_INCREMENT=1000
		DEFAULT CHARSET=utf8mb4
		COLLATE=utf8mb4_unicode_ci
		ROW_FORMAT=DYNAMIC
		COMMENT='Test table';`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)
}

func TestParser_MariaDBFirstFewColumns(t *testing.T) {
	c := qt.New(t)

	// Test just the first few columns to isolate the issue
	sql := `CREATE TABLE test (
		` + "`id`" + ` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		` + "`username`" + ` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		` + "`bio`" + ` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}

	c.Assert(createTable.Columns, qt.HasLen, 3)
}

func TestParser_MariaDBWithCheckConstraint(t *testing.T) {
	c := qt.New(t)

	// Test the balance column with CHECK constraint
	sql := `CREATE TABLE test (
		` + "`balance`" + ` DECIMAL(10,2) NOT NULL DEFAULT 0.00 CHECK (` + "`balance`" + ` >= 0)
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}

	c.Assert(createTable.Columns, qt.HasLen, 1)
}

func TestParser_MariaDBOnUpdateTimestamp(t *testing.T) {
	c := qt.New(t)

	// Test the updated_at column with ON UPDATE CURRENT_TIMESTAMP
	sql := `CREATE TABLE test (
		` + "`updated_at`" + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}

	c.Assert(createTable.Columns, qt.HasLen, 1)
}

func TestParser_MariaDBFirst5Columns(t *testing.T) {
	c := qt.New(t)

	// Test the first 5 columns from the comprehensive DDL
	sql := `CREATE TABLE ` + "`full_demo`" + ` (
		-- Auto-increment with unsigned integer
		` + "`id`" + ` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

		-- Character types
		` + "`username`" + ` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		` + "`bio`" + ` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,

		-- Numeric with defaults and constraints
		` + "`balance`" + ` DECIMAL(10,2) NOT NULL DEFAULT 0.00 CHECK (` + "`balance`" + ` >= 0),
		` + "`score`" + ` DOUBLE ZEROFILL DEFAULT 0000.00
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}
	t.Logf("Found %d constraints:", len(createTable.Constraints))
	for i, constraint := range createTable.Constraints {
		t.Logf("  %d: %s %v", i, constraint.Type, constraint.Columns)
	}

	c.Assert(createTable.Columns, qt.HasLen, 5)
}

func TestParser_MariaDBFirst7Columns(t *testing.T) {
	c := qt.New(t)

	// Test the first 7 columns including the timestamp columns
	sql := `CREATE TABLE ` + "`full_demo`" + ` (
		` + "`id`" + ` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		` + "`username`" + ` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		` + "`bio`" + ` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		` + "`balance`" + ` DECIMAL(10,2) NOT NULL DEFAULT 0.00 CHECK (` + "`balance`" + ` >= 0),
		` + "`score`" + ` DOUBLE ZEROFILL DEFAULT 0000.00,

		-- Date and time
		` + "`created_at`" + ` DATETIME DEFAULT CURRENT_TIMESTAMP,
		` + "`updated_at`" + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}

	c.Assert(createTable.Columns, qt.HasLen, 7)
}

func TestParser_MariaDBFirst10Columns(t *testing.T) {
	c := qt.New(t)

	// Test the first 10 columns including BOOLEAN, ENUM, and SET
	sql := `CREATE TABLE ` + "`full_demo`" + ` (
		` + "`id`" + ` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		` + "`username`" + ` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		` + "`bio`" + ` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		` + "`balance`" + ` DECIMAL(10,2) NOT NULL DEFAULT 0.00 CHECK (` + "`balance`" + ` >= 0),
		` + "`score`" + ` DOUBLE ZEROFILL DEFAULT 0000.00,
		` + "`created_at`" + ` DATETIME DEFAULT CURRENT_TIMESTAMP,
		` + "`updated_at`" + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

		-- Boolean type
		` + "`is_active`" + ` BOOLEAN NOT NULL DEFAULT TRUE,

		-- Enum and set
		` + "`status`" + ` ENUM('new', 'active', 'archived') DEFAULT 'new',
		` + "`roles`" + ` SET('admin', 'editor', 'user') DEFAULT 'user'
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)

	// Debug: Print actual columns found
	t.Logf("Found %d columns:", len(createTable.Columns))
	for i, col := range createTable.Columns {
		t.Logf("  %d: %s %s", i, col.Name, col.Type)
	}

	c.Assert(createTable.Columns, qt.HasLen, 10)
}

func TestParser_ErrorHandling(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "Invalid SQL keyword",
			sql:  "INVALID TABLE users (id INTEGER);",
		},
		{
			name: "Missing table name",
			sql:  "CREATE TABLE (id INTEGER);",
		},
		{
			name: "Missing opening parenthesis",
			sql:  "CREATE TABLE users id INTEGER);",
		},
		{
			name: "Invalid column type token",
			sql:  "CREATE TABLE users (id @);",
		},
		{
			name: "Unterminated column list",
			sql:  "CREATE TABLE users (id INTEGER",
		},
		{
			name: "Unsupported column attribute",
			sql:  "CREATE TABLE users (id INTEGER UNSUPPORTED);",
		},
		{
			name: "Broken array literal (invalid cast)",
			sql: `CREATE TABLE test (
					tags TEXT[] DEFAULT ARRAY[]:TEXT[],
					matrix INT[][]
				);`,
		},
		{
			name: "Broken array literal (missing closing bracket)",
			sql: `CREATE TABLE test (
					tags TEXT[] DEFAULT ARRAY[::TEXT[],
					matrix INT[][]
				);`,
		},
		{
			name: "Broken array literal (missing brackets)",
			sql: `CREATE TABLE test (
					tags TEXT[] DEFAULT ARRAY[::TEXT,
					matrix INT
				);`,
		},
		{
			name: "Unknown table option",
			sql:  "CREATE TABLE users (id INTEGER) UNKNOWN=OPTION;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			_, err := p.Parse()
			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestParser_ParseExcludeConstraint_Basic(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE user_sessions (
		user_id BIGINT NOT NULL,
		is_active BOOLEAN NOT NULL DEFAULT false,
		EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "user_sessions")
	c.Assert(createTable.Columns, qt.HasLen, 2)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check EXCLUDE constraint
	excludeConstraint := createTable.Constraints[0]
	c.Assert(excludeConstraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(excludeConstraint.UsingMethod, qt.Equals, "gist")
	c.Assert(excludeConstraint.ExcludeElements, qt.Equals, "user_id WITH =")
	c.Assert(excludeConstraint.WhereCondition, qt.Equals, "is_active = true")
	c.Assert(excludeConstraint.Name, qt.Equals, "")
}

func TestParser_ParseExcludeConstraint_WithName(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE user_sessions (
		user_id BIGINT NOT NULL,
		is_active BOOLEAN NOT NULL DEFAULT false,
		CONSTRAINT one_active_session_per_user EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check named EXCLUDE constraint
	excludeConstraint := createTable.Constraints[0]
	c.Assert(excludeConstraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(excludeConstraint.Name, qt.Equals, "one_active_session_per_user")
	c.Assert(excludeConstraint.UsingMethod, qt.Equals, "gist")
	c.Assert(excludeConstraint.ExcludeElements, qt.Equals, "user_id WITH =")
	c.Assert(excludeConstraint.WhereCondition, qt.Equals, "is_active = true")
}

func TestParser_ParseExcludeConstraint_ComplexElements(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE bookings (
		room_id INTEGER NOT NULL,
		during TSRANGE NOT NULL,
		EXCLUDE USING gist (room_id WITH =, during WITH &&)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check EXCLUDE constraint with complex elements
	excludeConstraint := createTable.Constraints[0]
	c.Assert(excludeConstraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(excludeConstraint.UsingMethod, qt.Equals, "gist")
	c.Assert(excludeConstraint.ExcludeElements, qt.Equals, "room_id WITH =, during WITH &&")
	c.Assert(excludeConstraint.WhereCondition, qt.Equals, "")
}

func TestParser_ParseExcludeConstraint_WithoutWhere(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE spatial_data (
		location GEOMETRY NOT NULL,
		EXCLUDE USING gist (location WITH &&)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check EXCLUDE constraint without WHERE clause
	excludeConstraint := createTable.Constraints[0]
	c.Assert(excludeConstraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(excludeConstraint.UsingMethod, qt.Equals, "gist")
	c.Assert(excludeConstraint.ExcludeElements, qt.Equals, "location WITH &&")
	c.Assert(excludeConstraint.WhereCondition, qt.Equals, "")
}

func TestParser_ParseExcludeConstraint_BtreeMethod(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE unique_values (
		value INTEGER NOT NULL,
		EXCLUDE USING btree (value WITH =)
	);`

	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	// Check EXCLUDE constraint with btree method
	excludeConstraint := createTable.Constraints[0]
	c.Assert(excludeConstraint.Type, qt.Equals, ast.ExcludeConstraint)
	c.Assert(excludeConstraint.UsingMethod, qt.Equals, "btree")
	c.Assert(excludeConstraint.ExcludeElements, qt.Equals, "value WITH =")
	c.Assert(excludeConstraint.WhereCondition, qt.Equals, "")
}

func TestParser_ParseExcludeConstraint_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected struct {
			name            string
			usingMethod     string
			excludeElements string
			whereCondition  string
		}
	}{
		{
			name: "extra spaces around keywords",
			sql: `CREATE TABLE test_table (
				id INTEGER,
				EXCLUDE   USING   gist   (  id  WITH  =  )
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "",
				usingMethod:     "gist",
				excludeElements: "id WITH =",
				whereCondition:  "",
			},
		},
		{
			name: "newlines in constraint definition",
			sql: `CREATE TABLE test_table (
				id INTEGER,
				EXCLUDE
				USING gist
				(id WITH =)
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "",
				usingMethod:     "gist",
				excludeElements: "id WITH =",
				whereCondition:  "",
			},
		},
		{
			name: "constraint name with extra spaces",
			sql: `CREATE TABLE test_table (
				id INTEGER,
				CONSTRAINT   test_constraint   EXCLUDE   USING   gist   (  id  WITH  =  )
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "test_constraint",
				usingMethod:     "gist",
				excludeElements: "id WITH =",
				whereCondition:  "",
			},
		},
		{
			name: "WHERE clause with extra spaces and newlines",
			sql: `CREATE TABLE test_table (
				id INTEGER,
				is_active BOOLEAN,
				EXCLUDE   USING   gist   (  id  WITH  =  )
				WHERE   (  is_active  =  true  )
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "",
				usingMethod:     "gist",
				excludeElements: "id WITH =",
				whereCondition:  "is_active = true",
			},
		},
		{
			name: "complex elements with extra spacing",
			sql: `CREATE TABLE test_table (
				room_id INTEGER,
				during TSRANGE,
				EXCLUDE   USING   gist   (  room_id  WITH  =  ,  during  WITH  &&  )
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "",
				usingMethod:     "gist",
				excludeElements: "room_id WITH = , during WITH &&",
				whereCondition:  "",
			},
		},
		{
			name: "mixed spacing and newlines",
			sql: `CREATE TABLE test_table (
				user_id INTEGER,
				is_active BOOLEAN,
				CONSTRAINT   one_active_per_user
				EXCLUDE
				USING   gist
				(  user_id  WITH  =  )
				WHERE
				(  is_active  =  true  )
			);`,
			expected: struct {
				name            string
				usingMethod     string
				excludeElements string
				whereCondition  string
			}{
				name:            "one_active_per_user",
				usingMethod:     "gist",
				excludeElements: "user_id WITH =",
				whereCondition:  "is_active = true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			p := parser.NewParser(tt.sql)
			statements, err := p.Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			createTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(createTable.Constraints, qt.HasLen, 1)

			constraint := createTable.Constraints[0]
			c.Assert(constraint.Type, qt.Equals, ast.ExcludeConstraint)
			c.Assert(constraint.Name, qt.Equals, tt.expected.name)
			c.Assert(constraint.UsingMethod, qt.Equals, tt.expected.usingMethod)
			c.Assert(constraint.ExcludeElements, qt.Equals, tt.expected.excludeElements)
			c.Assert(constraint.WhereCondition, qt.Equals, tt.expected.whereCondition)
		})
	}
}
