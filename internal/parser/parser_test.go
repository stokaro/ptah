package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/parser"
)

func TestNewParser(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE TABLE users (id INTEGER);")
	c.Assert(p, qt.IsNotNil)
	c.Assert(p.Dialect(), qt.Equals, "")
	c.Assert(p.Capabilities(), qt.IsNil)
}

func TestNewParser_WithDialectAndCapabilities(t *testing.T) {
	c := qt.New(t)

	caps := capability.Postgres13()
	p := parser.NewParser(
		"CREATE TABLE users (id INTEGER);",
		parser.WithDialect("postgresql"),
		parser.WithCapabilities(caps),
	)

	c.Assert(p.Dialect(), qt.Equals, platform.Postgres)
	c.Assert(p.Capabilities().Has(capability.CreateOrReplaceTrigger), qt.IsFalse)

	caps[capability.CreateOrReplaceTrigger] = true
	c.Assert(p.Capabilities().Has(capability.CreateOrReplaceTrigger), qt.IsFalse)

	parserCaps := p.Capabilities()
	parserCaps[capability.CreateOrReplaceTrigger] = true
	c.Assert(p.Capabilities().Has(capability.CreateOrReplaceTrigger), qt.IsFalse)
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

func TestParser_ParseCreateTable_ColumnCharsetCollate(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE users (name VARCHAR(255) CHARACTER SET hebrew COLLATE hebrew_general_ci NOT NULL);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].Charset, qt.Equals, "hebrew")
	c.Assert(createTable.Columns[0].Collate, qt.Equals, "hebrew_general_ci")
	c.Assert(createTable.Columns[0].Nullable, qt.IsFalse)
}

func TestParser_ParseCreateTable_PostgreSQLPartitionBy(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE metrics (x integer NOT NULL, y integer NOT NULL) PARTITION BY RANGE (x, (floor(y)), (y * 2));`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Partition, qt.DeepEquals, &ast.PartitionSpec{
		Type: "RANGE",
		Parts: []ast.PartitionPart{
			{Name: "x"},
			{Expr: "floor(y)"},
			{Expr: "y * 2"},
		},
	})
}

func TestParser_ParseCreateTable_RejectsDuplicatePartitionBy(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE metrics (x integer NOT NULL) PARTITION BY RANGE (x) PARTITION BY LIST (x);`
	p := parser.NewParser(sql)

	_, err := p.Parse()
	c.Assert(err, qt.ErrorMatches, `.*duplicate PARTITION BY clause.*`)
}

func TestParser_ParseCreateIndexConcurrently(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE INDEX CONCURRENTLY idx_users_email ON users (email);")

	statements, err := p.Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
	c.Assert(index.Concurrently, qt.IsTrue)
	c.Assert(index.Unique, qt.IsFalse)
}

func TestParser_ParseCreateUniqueIndexConcurrently(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE UNIQUE INDEX CONCURRENTLY idx_users_email ON users (email);")

	statements, err := p.Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Concurrently, qt.IsTrue)
	c.Assert(index.Unique, qt.IsTrue)
}

func TestParser_ParseCreateIndexConcurrentlyIfNotExists(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email);")

	statements, err := p.Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Concurrently, qt.IsTrue)
	c.Assert(index.IfNotExists, qt.IsTrue)
}

func TestParser_ParseCreateIndexIfNotExists(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);")

	statements, err := p.Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.IfNotExists, qt.IsTrue)
	c.Assert(index.Concurrently, qt.IsFalse)
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

func TestParser_ParseCreateTable_UniqueNullsNotDistinct(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE users (
		c INTEGER,
		CONSTRAINT users_c_key UNIQUE NULLS NOT DISTINCT (c)
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)
	constraint := createTable.Constraints[0]
	c.Assert(constraint.Type, qt.Equals, ast.UniqueConstraint)
	c.Assert(constraint.Name, qt.Equals, "users_c_key")
	c.Assert(constraint.Columns, qt.DeepEquals, []string{"c"})
	c.Assert(constraint.NullsDistinct, qt.IsNotNil)
	c.Assert(*constraint.NullsDistinct, qt.IsFalse)
}

func TestParser_ParseCreateTable_WithCompositeForeignKey(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE orders (
		tenant_id INTEGER,
		customer_id INTEGER,
		CONSTRAINT fk_orders_customer FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)
	constraint := createTable.Constraints[0]
	c.Assert(constraint.Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(constraint.Columns, qt.DeepEquals, []string{"tenant_id", "customer_id"})
	c.Assert(constraint.Reference.Table, qt.Equals, "customers")
	c.Assert(constraint.Reference.Column, qt.Equals, "tenant_id")
	c.Assert(constraint.Reference.Columns, qt.DeepEquals, []string{"tenant_id", "id"})
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

func TestParser_ParseCreateTable_WithSQLiteTableOptions(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE events (id integer NOT NULL, PRIMARY KEY (id)) WITHOUT ROWID, STRICT;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "events")
	c.Assert(createTable.Options["WITHOUT_ROWID"], qt.Equals, "true")
	c.Assert(createTable.Options["STRICT"], qt.Equals, "true")
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

func TestParser_ParseMySQLHashLineComments(t *testing.T) {
	c := qt.New(t)

	sql := `# CREATE TABLE skipped(id int);
CREATE TABLE t1(id int);
SELECT * FROM t1 # inline comment
;
# another skipped statement
CREATE TABLE t2(id int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)
	c.Assert(statements.Statements[0].(*ast.CreateTableNode).Name, qt.Equals, "t1")
	c.Assert(statements.Statements[1].(*ast.CreateTableNode).Name, qt.Equals, "t2")
}

func TestParser_ParseMySQLClientDelimiters(t *testing.T) {
	c := qt.New(t)

	sql := `delimiter //
CREATE TABLE t2 (a int) //
delimiter 'DONE'
CREATE TABLE t3 (a int) DONE
delimiter //
CREATE TABLE t4 (a text DEFAULT 'a//b')//
delimiter \n\n
CREATE TABLE t5 (a int)

delimiter ;
SHOW TABLES;
DROP TABLE t2, t3;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 5)
	c.Assert(statements.Statements[0].(*ast.CreateTableNode).Name, qt.Equals, "t2")
	c.Assert(statements.Statements[1].(*ast.CreateTableNode).Name, qt.Equals, "t3")
	createWithDefault := statements.Statements[2].(*ast.CreateTableNode)
	c.Assert(createWithDefault.Name, qt.Equals, "t4")
	c.Assert(createWithDefault.Columns[0].Default.Value, qt.Equals, "'a//b'")
	c.Assert(statements.Statements[3].(*ast.CreateTableNode).Name, qt.Equals, "t5")
	c.Assert(statements.Statements[4].(*ast.DropTableNode).Name, qt.Equals, "t2")
	c.Assert(statements.Statements[4].(*ast.DropTableNode).TableNames(), qt.DeepEquals, []string{"t2", "t3"})
}

func TestParser_ParseMySQLClientDelimiterVariants(t *testing.T) {
	c := qt.New(t)

	sql := `delimiter delimiter
SELECT * FROM t1delimiter
delimiter @@
ALTER TABLE t ADD COLUMN c@@
delimiter ;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Name, qt.Equals, "t")

	addOp, ok := alterTable.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(addOp.Column.Name, qt.Equals, "c")
	c.Assert(addOp.Column.Type, qt.Equals, "")
}

func TestParser_ParseDoesNotTreatGotoAsGoBatchSeparator(t *testing.T) {
	c := qt.New(t)

	p := parser.NewParser("GOTO label;")
	_, err := p.Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported SQL statement: GOTO at position 0`)
}

func TestParser_ParsePostgresDoBlockAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `DO $$
BEGIN
    CREATE TYPE some_type AS ENUM ('one', 'two');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$$;
CREATE INDEX idx_users_name ON users (name);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "DO $$")
	c.Assert(raw.SQL, qt.Contains, "CREATE TYPE some_type AS ENUM ('one', 'two');")
	c.Assert(raw.SQL, qt.Contains, "END;")

	index, ok := statements.Statements[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_name")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"name"})
}

func TestParser_ParsePostgresDialectDoBlockAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `DO $do$
DECLARE
    created boolean;
BEGIN
    IF NOT created THEN
        RAISE NOTICE 'missing';
    END IF;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$do$;
CREATE INDEX idx_users_name ON users (name);`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	block, ok := statements.Statements[0].(*ast.PostgresDoBlockNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(block.SQL, qt.Contains, "DO $do$")
	c.Assert(block.Language, qt.Equals, "plpgsql")
	c.Assert(block.Body.Delimiter, qt.Equals, "$do$")
	c.Assert(block.Body.Language, qt.Equals, "plpgsql")
	c.Assert(block.Body.SQL, qt.Contains, "DECLARE")
	c.Assert(block.Body.SQL, qt.Contains, "RAISE NOTICE 'missing';")
	c.Assert(postgresRoutineStatementKinds(block.Body.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementDeclaration,
		ast.PostgresRoutineStatementIf,
		ast.PostgresRoutineStatementException,
	})
	rendered, err := renderer.RenderSQL(platform.Postgres, block)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "DO $do$")
	c.Assert(rendered, qt.Contains, "RAISE NOTICE 'missing';")

	index, ok := statements.Statements[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_name")
}

func TestParser_ParsePostgresExecuteFunctionTriggerAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch_updated_at('updated_at');
CREATE TABLE audit_log (id INTEGER);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE TRIGGER set_updated_at")
	c.Assert(raw.SQL, qt.Contains, "EXECUTE FUNCTION touch_updated_at('updated_at')")

	table, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, "audit_log")
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
	c.Assert(createTable.Columns[1].Charset, qt.Equals, "utf8")
	c.Assert(createTable.Columns[1].Nullable, qt.IsFalse)
	c.Assert(createTable.Columns[2].Charset, qt.Equals, "utf8")
	c.Assert(createTable.Columns[2].Collate, qt.Equals, "utf8_unicode_ci")
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

func TestParser_ParseCreateTable_LatinUTF8Identifier(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE café (id INT, naïve TEXT);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Name, qt.Equals, "café")
	c.Assert(createTable.Columns, qt.HasLen, 2)
	c.Assert(createTable.Columns[0].Name, qt.Equals, "id")
	c.Assert(createTable.Columns[1].Name, qt.Equals, "naïve")
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

func TestParser_ParsePostgresDialectSQLFunctionRoutineBodyMetadata(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.add_one(value integer)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$ SELECT value + 1; $$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Body, qt.Equals, " SELECT value + 1; ")
	c.Assert(createFunction.BodyKind, qt.Equals, ast.FunctionBodyQuoted)
	c.Assert(createFunction.Language, qt.Equals, "sql")
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(*createFunction.RoutineBody, qt.DeepEquals, ast.PostgresRoutineBody{
		SQL:       " SELECT value + 1; ",
		Delimiter: "$$",
		Language:  "sql",
		Statements: []ast.PostgresRoutineStatement{{
			Kind: ast.PostgresRoutineStatementRaw,
			SQL:  "SELECT value + 1;",
		}},
	})
}

func TestParser_ParsePostgresDialectFunctionRoutineBodyMetadata(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.touch_user(user_id integer)
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
    changed integer;
BEGIN
    EXECUTE 'SELECT 1';
    RETURN user_id;
END;
$fn$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(createFunction.RoutineBody.Delimiter, qt.Equals, "$fn$")
	c.Assert(createFunction.RoutineBody.Language, qt.Equals, "plpgsql")
	c.Assert(createFunction.RoutineBody.SQL, qt.Contains, "DECLARE")
	c.Assert(createFunction.RoutineBody.SQL, qt.Contains, "EXECUTE 'SELECT 1';")
	c.Assert(createFunction.RoutineBody.SQL, qt.Contains, "RETURN user_id;")
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementDeclaration,
		ast.PostgresRoutineStatementExecute,
		ast.PostgresRoutineStatementReturn,
	})
}

func TestParser_ParseGenericFunctionDoesNotClaimPostgresRoutineBody(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.add_one(value integer)
RETURNS integer
LANGUAGE sql
AS $$ SELECT value + 1; $$;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNil)
}

func TestParser_ParsePostgresDialectFunctionTaggedDollarQuoteBoundary(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.has_inner_delimiter()
RETURNS void
AS $fn$
BEGIN
    PERFORM '$$ not the delimiter';
END;
$fn$
LANGUAGE plpgsql;
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(createFunction.RoutineBody.Delimiter, qt.Equals, "$fn$")
	c.Assert(createFunction.RoutineBody.SQL, qt.Contains, "PERFORM '$$ not the delimiter';")
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementPerform,
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_fn")
}

func TestParser_ParsePostgresDialectFunctionNestedBlockRoutineBody(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.nested_block()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    BEGIN
        PERFORM 1;
    END;
    RETURN;
END;
$fn$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementBlock,
		ast.PostgresRoutineStatementReturn,
	})
	c.Assert(createFunction.RoutineBody.Statements[0].SQL, qt.Contains, "PERFORM 1;")
}

func TestParser_ParsePostgresDialectFunctionSQLForUpdateDoesNotOpenLoop(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.lock_then_return()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    PERFORM 1 FROM users FOR UPDATE;
    RETURN;
END;
$fn$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementPerform,
		ast.PostgresRoutineStatementReturn,
	})
	c.Assert(createFunction.RoutineBody.Statements[0].SQL, qt.Contains, "FOR UPDATE")
}

func TestParser_ParsePostgresDialectFunctionForLoopStatement(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.loop_then_return()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    FOR i IN 1..3 LOOP
        PERFORM i;
    END LOOP;
    RETURN;
END;
$fn$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementLoop,
		ast.PostgresRoutineStatementReturn,
	})
	c.Assert(createFunction.RoutineBody.Statements[0].SQL, qt.Contains, "PERFORM i;")
}

func TestParser_ParsePostgresDialectTriggerFunctionBodyAndReference(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$fn$;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Returns, qt.Equals, "trigger")
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(createFunction.RoutineBody.SQL, qt.Contains, "NEW.updated_at := now();")
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementRaw,
		ast.PostgresRoutineStatementReturn,
	})

	rawTrigger, ok := statements.Statements[1].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(rawTrigger.SQL, qt.Contains, "EXECUTE FUNCTION public.touch_updated_at()")
}

func TestParser_ParsePostgresDialectProcedureRoutineBodyMetadata(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE public.touch_user(IN user_id integer)
LANGUAGE plpgsql
AS $proc$
BEGIN
    PERFORM user_id;
END;
$proc$;
CREATE TABLE after_proc (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.PostgresRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Dialect, qt.Equals, platform.Postgres)
	c.Assert(routine.Name, qt.Equals, "public.touch_user")
	c.Assert(routine.Parameters, qt.Equals, "IN user_id integer")
	c.Assert(routine.Language, qt.Equals, "plpgsql")
	c.Assert(routine.Body.Delimiter, qt.Equals, "$proc$")
	c.Assert(routine.Body.SQL, qt.Contains, "PERFORM user_id;")
	c.Assert(postgresRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementPerform,
	})
	rendered, err := renderer.RenderSQL(platform.Postgres, routine)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "CREATE PROCEDURE public.touch_user")
	c.Assert(rendered, qt.Contains, "PERFORM user_id;")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParsePostgresDialectProcedureBodyUsesASString(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE public.greet(IN name text DEFAULT 'guest')
LANGUAGE plpgsql
AS $proc$
BEGIN
    RAISE NOTICE '%', name;
END;
$proc$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.PostgresRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Parameters, qt.Equals, "IN name text DEFAULT 'guest'")
	c.Assert(routine.Body.SQL, qt.Contains, "RAISE NOTICE '%', name;")
	c.Assert(routine.Body.SQL, qt.Not(qt.Equals), "guest")
}

func TestParser_ParsePostgresDialectProcedureAtomicBody(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE public.insert_data(a integer, b integer)
LANGUAGE SQL
BEGIN ATOMIC
    INSERT INTO tbl VALUES (a);
    INSERT INTO tbl VALUES (b);
END;
CREATE TABLE after_atomic (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.PostgresRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Language, qt.Equals, "sql")
	c.Assert(routine.Body.SQL, qt.Contains, "BEGIN ATOMIC")
	c.Assert(routine.Body.SQL, qt.Contains, "INSERT INTO tbl VALUES (b);")
	c.Assert(routine.Body.Statements, qt.DeepEquals, []ast.PostgresRoutineStatement{{
		Kind: ast.PostgresRoutineStatementRaw,
		SQL:  routine.Body.SQL,
	}})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_atomic")
}

func TestParser_ParsePostgresRoutineBodyLoopDepth(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.looping()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    FOR i IN 1..2 LOOP
        RAISE NOTICE '%', i;
    END LOOP;
    WHILE false LOOP
        RAISE NOTICE 'never';
    END LOOP;
    FOREACH item IN ARRAY ARRAY[1, 2] LOOP
        RAISE NOTICE '%', item;
    END LOOP;
    RETURN;
END;
$fn$;`
	p := parser.NewParser(sql, parser.WithDialect(platform.Postgres))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.RoutineBody, qt.IsNotNil)
	c.Assert(postgresRoutineStatementKinds(createFunction.RoutineBody.Statements), qt.DeepEquals, []ast.PostgresRoutineStatementKind{
		ast.PostgresRoutineStatementLoop,
		ast.PostgresRoutineStatementLoop,
		ast.PostgresRoutineStatementLoop,
		ast.PostgresRoutineStatementReturn,
	})
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

func TestParser_ParseCreateFunctionWithReturnBody(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE FUNCTION public.first_int(value text[]) RETURNS int RETURN value[1]::int;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "public.first_int")
	c.Assert(createFunction.Parameters, qt.Equals, "value text[]")
	c.Assert(createFunction.Returns, qt.Equals, "int")
	c.Assert(createFunction.BodyKind, qt.Equals, ast.FunctionBodyReturn)
	c.Assert(createFunction.Body, qt.Equals, "value[1]::int")
}

func TestParser_ParseCreateFunctionWithAtomicBody(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION public.is_even(x int) RETURNS boolean
LANGUAGE SQL
STABLE
BEGIN ATOMIC
SELECT CASE WHEN x % 2 = 0 THEN true ELSE false END;
END;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "public.is_even")
	c.Assert(createFunction.Parameters, qt.Equals, "x int")
	c.Assert(createFunction.Returns, qt.Equals, "boolean")
	c.Assert(createFunction.Language, qt.Equals, "SQL")
	c.Assert(createFunction.Volatility, qt.Equals, "STABLE")
	c.Assert(createFunction.BodyKind, qt.Equals, ast.FunctionBodyAtomic)
	c.Assert(createFunction.Body, qt.Contains, "BEGIN ATOMIC")
	c.Assert(createFunction.Body, qt.Contains, "SELECT CASE WHEN x % 2 = 0 THEN true ELSE false END;")
	c.Assert(createFunction.Body, qt.Contains, "END")
}

func TestParser_ParseMySQLCreateFunctionWithReturnBody(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE FUNCTION `add2` (a int, b int) RETURNS int DETERMINISTIC NO SQL RETURN a + b;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createFunction, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createFunction.Name, qt.Equals, "`add2`")
	c.Assert(createFunction.Parameters, qt.Equals, "a int, b int")
	c.Assert(createFunction.Returns, qt.Equals, "int")
	c.Assert(createFunction.BodyKind, qt.Equals, ast.FunctionBodyReturn)
	c.Assert(createFunction.Body, qt.Equals, "a + b")
}

func TestParser_ParseMySQLCreateFunctionWithCompoundBody(t *testing.T) {
	c := qt.New(t)

	sql := `DELIMITER |
CREATE FUNCTION fn1(x int) RETURNS int DETERMINISTIC
BEGIN
       INSERT INTO t1 VALUES (x);
       RETURN x+2;
END|
DELIMITER ;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE FUNCTION fn1(x int) RETURNS int DETERMINISTIC")
	c.Assert(raw.SQL, qt.Contains, "INSERT INTO t1 VALUES (x);")
	c.Assert(raw.SQL, qt.Contains, "RETURN x+2;")
}

func TestParser_ParseMySQLDialectCompoundFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `DELIMITER |
CREATE FUNCTION fn1(x int) RETURNS int DETERMINISTIC
BEGIN
       INSERT INTO t1 VALUES (x);
       RETURN x+2;
END|
DELIMITER ;`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MySQL)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "fn1")
	c.Assert(routine.Parameters, qt.Equals, "x int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Characteristics, qt.DeepEquals, []string{"DETERMINISTIC"})
	c.Assert(routine.SQL, qt.Contains, "CREATE FUNCTION fn1(x int) RETURNS int DETERMINISTIC")
	c.Assert(routine.Body.SQL, qt.Contains, "INSERT INTO t1 VALUES (x);")
	c.Assert(routine.Body.SQL, qt.Contains, "RETURN x+2;")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementInsert,
		ast.MySQLRoutineStatementReturn,
	})
}

func TestParser_ParseMariaDBDialectCompoundFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION fn1(x int) RETURNS int DETERMINISTIC
BEGIN
       RETURN x+2;
END;`
	p := parser.NewParser(sql, parser.WithDialect(platform.MariaDB))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MariaDB)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "fn1")
	c.Assert(routine.Parameters, qt.Equals, "x int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementReturn,
	})
}

func TestParser_ParseMySQLCreateFunctionWithNestedCompoundBody(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION CompFunc(eID INT) RETURNS INT
BEGIN
    DECLARE ts INT;
    DECLARE b INT;

    BEGIN
        SELECT SUM(s) INTO ts FROM sales WHERE e = eID;
    END;

    BEGIN
        IF ts > 10000 THEN
            SET b = 500;
        ELSEIF ts BETWEEN 5000 AND 10000 THEN
            SET b = 300;
        ELSE
            SET b = 0;
        END IF;
    END;

    RETURN b;
END;`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE FUNCTION CompFunc(eID INT) RETURNS INT")
	c.Assert(raw.SQL, qt.Contains, "END IF;")
	c.Assert(raw.SQL, qt.Contains, "RETURN b;")
}

func TestParser_ParseMySQLCreateFunctionWithDollarDelimiter(t *testing.T) {
	c := qt.New(t)

	sql := `DELIMITER $$
CREATE OR REPLACE FUNCTION gen_uuid() RETURNS VARCHAR(22)
BEGIN
    RETURN concat(
        date_format(NOW(6), '%Y%m%d%i%s%f'),
        ROUND(1 + RAND() * (100 - 2))
    );
END;$$
DELIMITER ;
CALL gen_uuid();`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE OR REPLACE FUNCTION gen_uuid() RETURNS VARCHAR(22)")
	c.Assert(raw.SQL, qt.Contains, "date_format(NOW(6), '%Y%m%d%i%s%f')")
}

func TestParser_ParseSQLServerInlineTableFunctionAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f2](@a as INT, @b as INT = 1)
RETURNS TABLE
AS RETURN SELECT @a as [a], @b as [b], (@a+@b)*2 as [p], @a*@b as [s];
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE FUNCTION [f2](@a as INT, @b as INT = 1)")
	c.Assert(raw.SQL, qt.Contains, "RETURNS TABLE")
	c.Assert(raw.SQL, qt.Contains, "AS RETURN SELECT @a as [a]")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_fn")
}

func TestParser_ParseSQLServerDialectInlineTableFunctionMetadata(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f2](@a as INT, @b as INT = 1)
RETURNS TABLE
AS RETURN SELECT @a as [a], @b as [b];`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Name, qt.Equals, "[f2]")
	c.Assert(routine.Parameters, qt.Equals, "@a as INT, @b as INT = 1")
	c.Assert(routine.Returns, qt.Equals, "TABLE")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormInlineTableValuedFunction)
	c.Assert(routine.Body.SQL, qt.Contains, "RETURN SELECT @a as [a]")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementReturn,
	})
}

func TestParser_ParseSQLServerMultiStatementTableFunctionAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f3] (@a int, @b int = 1) RETURNS @t1 TABLE ([c1] int NOT NULL, [c2] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, [c3] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS DEFAULT N'G' NULL, [c4] int NOT NULL, PRIMARY KEY CLUSTERED ([c1] ASC), INDEX [idx] NONCLUSTERED ([c2] ASC), UNIQUE NONCLUSTERED ([c2] ASC, [c3] DESC), UNIQUE NONCLUSTERED ([c3] DESC, [c4] ASC), CHECK ([c4]>(0))) AS BEGIN
  INSERT @t1
  SELECT 1 AS [c1], 'A' AS [c2], NULL AS [c3], @a * @a + @b AS [c4];
RETURN
END;
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE FUNCTION [f3]")
	c.Assert(raw.SQL, qt.Contains, "RETURNS @t1 TABLE")
	c.Assert(raw.SQL, qt.Contains, "INSERT @t1")
	c.Assert(raw.SQL, qt.Contains, "RETURN\nEND")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_fn")
}

func TestParser_ParseSQLServerDialectMultiStatementTableFunctionMetadata(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f3] (@a int, @b int = 1) RETURNS @t1 TABLE ([c1] int NOT NULL, [double] AS [c1] * 2) AS BEGIN
  INSERT @t1
  SELECT 1 AS [c1];
RETURN;
END`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Name, qt.Equals, "[f3]")
	c.Assert(routine.Parameters, qt.Equals, "@a int, @b int = 1")
	c.Assert(routine.Returns, qt.Equals, "@t1 TABLE ([c1] int NOT NULL, [double] AS [c1] * 2)")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormMultiStatementTableFunction)
	c.Assert(routine.Body.SQL, qt.Contains, "INSERT @t1")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementInsert,
		ast.SQLServerRoutineStatementReturn,
	})
}

func TestParser_ParseSQLServerDialectReturnTableDefaultWithAS(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f_cast] (@a int) RETURNS @t TABLE ([c] int DEFAULT CAST(1 AS int)) AS BEGIN
  INSERT @t
  SELECT @a AS [c];
RETURN;
END`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Returns, qt.Equals, "@t TABLE ([c] int DEFAULT CAST(1 AS int))")
	c.Assert(routine.Body.SQL, qt.Contains, "INSERT @t")
	c.Assert(routine.Body.SQL, qt.Contains, "RETURN")
}

func TestParser_ParseSQLServerDialectTableFunctionIgnoresBracketedKeywords(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f_keywords] (@a int) RETURNS @t TABLE ([AS] int NOT NULL, [END]]value] int NOT NULL) AS BEGIN
  INSERT @t
  SELECT @a AS [AS], @a AS [END]]value];
RETURN;
END`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Returns, qt.Equals, "@t TABLE ([AS] int NOT NULL, [END]]value] int NOT NULL)")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormMultiStatementTableFunction)
	c.Assert(routine.Body.SQL, qt.Contains, "INSERT @t")
	c.Assert(routine.Body.SQL, qt.Contains, "SELECT @a AS [AS], @a AS [END]]value]")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementInsert,
		ast.SQLServerRoutineStatementReturn,
	})
}

func TestParser_ParseSQLServerGoDelimitedFunctionsAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `-- atlas:delimiter \nGO

CREATE FUNCTION [f3] (@a int, @b int = 1) RETURNS @t1 TABLE ([c1] int NOT NULL) AS BEGIN
  INSERT @t1
  SELECT 1 AS [c1];
RETURN
END
GO
CREATE FUNCTION [f4] (@a int) RETURNS TABLE
AS RETURN SELECT @a as [a];`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	first, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(first.SQL, qt.Contains, "CREATE FUNCTION [f3]")
	c.Assert(first.SQL, qt.Contains, "SELECT 1 AS [c1];")
	c.Assert(first.SQL, qt.Not(qt.Contains), "GO")

	second, ok := statements.Statements[1].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(second.SQL, qt.Contains, "CREATE FUNCTION [f4]")
	c.Assert(second.SQL, qt.Contains, "AS RETURN SELECT @a as [a];")
}

func TestParser_ParseSQLServerFunctionWithCaseExpressionAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f5] (@a int) RETURNS @t TABLE ([result] int NOT NULL) AS BEGIN
  INSERT @t
  SELECT CASE WHEN @a > 0 THEN @a ELSE 0 END AS [result];
RETURN
END
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CASE WHEN @a > 0 THEN @a ELSE 0 END")
	c.Assert(raw.SQL, qt.Contains, "RETURN\nEND")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")
}

func TestParser_ParseSQLServerFunctionWithBracketedEndIdentifierAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [f6] (@a int) RETURNS @t TABLE ([END]]value] int NOT NULL) AS BEGIN
  INSERT @t
  SELECT @a AS [END]]value];
RETURN
END
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "RETURNS @t TABLE ([END]]value] int NOT NULL)")
	c.Assert(raw.SQL, qt.Contains, "SELECT @a AS [END]]value];")
	c.Assert(raw.SQL, qt.Contains, "RETURN\nEND")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")
}

func TestParser_ParseSQLServerDialectBracketedScalarFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION [dbo].[score](@user_id int)
RETURNS int
AS
BEGIN
    DECLARE @score int;
    SET @score = 1;
    IF @score > 0
    BEGIN
        SET @score = @score + 1;
    END;
    RETURN @score;
END
GO
CREATE TABLE after_fn (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.SQLServer))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.SQLServer)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "[dbo].[score]")
	c.Assert(routine.Parameters, qt.Equals, "@user_id int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormScalarFunction)
	c.Assert(routine.SQL, qt.Not(qt.Contains), "GO")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementDeclaration,
		ast.SQLServerRoutineStatementAssignment,
		ast.SQLServerRoutineStatementIf,
		ast.SQLServerRoutineStatementReturn,
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_fn")
}

func TestParser_ParseSQLServerDialectCreateOrAlterFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE OR ALTER FUNCTION dbo.score(@returns int)
RETURNS int
AS
BEGIN
    RETURN @returns;
END`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Name, qt.Equals, "dbo.score")
	c.Assert(routine.Parameters, qt.Equals, "@returns int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.SQL, qt.Contains, "CREATE OR ALTER FUNCTION dbo.score")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementReturn,
	})
}

func TestParser_ParseCreateOrAlterRequiresSQLServerDialect(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(`CREATE OR ALTER FUNCTION dbo.score() RETURNS int AS BEGIN RETURN 1; END`).Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported CREATE OR ALTER outside SQL Server dialect at position \d+`)
}

func TestParser_ParseSQLServerDialectUnbracketedFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION dbo.f7 (@a int) RETURNS int AS BEGIN
  RETURN @a
	END
CREATE TABLE after_fn (id int);`
	_, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNotNil)

	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.SQLServer)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "dbo.f7")
	c.Assert(routine.Parameters, qt.Equals, "@a int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormScalarFunction)
	c.Assert(routine.SQL, qt.Contains, "CREATE FUNCTION dbo.f7")
	c.Assert(routine.SQL, qt.Contains, "RETURN @a")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")
	c.Assert(routine.Body.SQL, qt.Contains, "RETURN @a")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementReturn,
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_fn")
}

func TestParser_ParseSQLServerDialectProcedureParametersAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE [dbo].[touch_user] @user_id int, @enabled bit = (1) AS
BEGIN
  SELECT @user_id;
END`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Name, qt.Equals, "[dbo].[touch_user]")
	c.Assert(routine.Parameters, qt.Equals, "@user_id int, @enabled bit = (1)")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormProcedure)
	c.Assert(routine.Body.SQL, qt.Contains, "SELECT @user_id")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementSelect,
	})
}

func TestParser_ParseSQLServerDialectProcAliasAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROC [dbo].[touch_user] @user_id int AS SELECT @user_id;`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Name, qt.Equals, "[dbo].[touch_user]")
	c.Assert(routine.Parameters, qt.Equals, "@user_id int")
	c.Assert(routine.Body.SQL, qt.Equals, "SELECT @user_id")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementSelect,
	})
}

func TestParser_ParseSQLServerDialectProcedureWithoutBeginSplitsStatements(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE [dbo].[list_users] AS
SELECT 1 AS [first];
SELECT 2 AS [second];`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Name, qt.Equals, "[dbo].[list_users]")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementSelect,
		ast.SQLServerRoutineStatementSelect,
	})
	c.Assert(routine.Body.Statements[0].SQL, qt.Contains, "SELECT 1 AS [first]")
	c.Assert(routine.Body.Statements[1].SQL, qt.Contains, "SELECT 2 AS [second]")
}

func TestParser_ParseSQLServerDialectProcedureWithoutSemicolonsSplitsSupportedStatements(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE [dbo].[score_user] AS
DECLARE @score int
SET @score = 1
RETURN @score
GO`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementDeclaration,
		ast.SQLServerRoutineStatementAssignment,
		ast.SQLServerRoutineStatementReturn,
	})
}

func TestParser_ParseProcAliasIsOnlyTypedInSQLServerDialect(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROC [dbo].[touch_user] @user_id int AS SELECT @user_id
GO
CREATE TABLE after_proc (id int);`
	for _, dialect := range []string{"", platform.Postgres, platform.MySQL} {
		statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
		c.Assert(err, qt.IsNil, qt.Commentf("dialect %q", dialect))
		c.Assert(statements.Statements, qt.HasLen, 2, qt.Commentf("dialect %q", dialect))

		raw, ok := statements.Statements[0].(*ast.RawSQLNode)
		c.Assert(ok, qt.IsTrue, qt.Commentf("dialect %q", dialect))
		c.Assert(raw.SQL, qt.Contains, "CREATE PROC [dbo].[touch_user]")
		c.Assert(raw.SQL, qt.Not(qt.Contains), "GO")
	}
}

func TestParser_ParseSQLServerDialectProcedureDoesNotTreatGoAliasAsBatch(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE dbo.p AS
SELECT go AS value, [go] AS bracketed;
GO
CREATE TABLE after_proc (id int);`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Body.SQL, qt.Contains, "SELECT go AS value")
	c.Assert(routine.Body.SQL, qt.Contains, "[go] AS bracketed")
	c.Assert(routine.Body.SQL, qt.Not(qt.Contains), "GO")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParseSQLServerDialectProcedureStopsAtGoBatch(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE dbo.p1 AS
  SELECT 1
GO /* deploy */
CREATE TABLE after_proc (id int);`
	statements, err := parser.NewParser(sql, parser.WithDialect("mssql")).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.SQLServer)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Name, qt.Equals, "dbo.p1")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormProcedure)
	c.Assert(routine.SQL, qt.Contains, "CREATE PROCEDURE dbo.p1")
	c.Assert(routine.SQL, qt.Contains, "SELECT 1")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "GO")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CREATE TABLE after_proc")
	c.Assert(routine.Body.SQL, qt.Equals, "SELECT 1")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementSelect,
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParseSQLServerDialectDoesNotStopAtGoIdentifier(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE FUNCTION dbo.go_alias(@go int)
RETURNS TABLE
AS RETURN SELECT @go AS go;
GO
CREATE TABLE after_fn (id int);`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Parameters, qt.Equals, "@go int")
	c.Assert(routine.Body.SQL, qt.Contains, "RETURN SELECT @go AS go")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CREATE TABLE after_fn")
}

func TestParser_ParseSQLServerDialectIgnoresNonBlockBeginEndStatements(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE dbo.conversation AS
BEGIN
  BEGIN TRANSACTION;
  END CONVERSATION @handle;
  SELECT 1;
END
GO
CREATE TABLE after_proc (id int);`
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.SQLServerRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Body.SQL, qt.Contains, "BEGIN TRANSACTION")
	c.Assert(routine.Body.SQL, qt.Contains, "END CONVERSATION @handle")
	c.Assert(sqlServerRoutineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.SQLServerRoutineStatementKind{
		ast.SQLServerRoutineStatementBlock,
		ast.SQLServerRoutineStatementRaw,
		ast.SQLServerRoutineStatementSelect,
	})
}

func TestParser_ParseCreateProcedureAsRawSQL(t *testing.T) {
	c := qt.New(t)

	sql := `DELIMITER //
CREATE PROCEDURE dorepeat(p1 INT)
BEGIN
    SET @x = 0;
    REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END
//
DELIMITER ;
CALL dorepeat(100);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE PROCEDURE dorepeat(p1 INT)")
	c.Assert(raw.SQL, qt.Contains, "REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;")
}

func TestParser_ParseCreateDefinerProcedureWithAtlasDelimiter(t *testing.T) {
	c := qt.New(t)

	sql := `-- atlas:delimiter \n-- end --\n

CREATE DEFINER='boring' PROCEDURE proc ()
    COMMENT 'ATLAS_DELIMITER'
    SQL SECURITY INVOKER
    NOT DETERMINISTIC
    MODIFIES SQL DATA
BEGIN
    UPDATE performance_schema.threads
    SET instrumented = 'YES'
    WHERE type = 'BACKGROUND';

    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' background thread', IF(@rows != 1, 's', '')) AS summary;
END

-- end --

CALL proc();`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE DEFINER='boring' PROCEDURE proc ()")
	c.Assert(raw.SQL, qt.Contains, "SQL SECURITY INVOKER")
	c.Assert(raw.SQL, qt.Contains, "IF(@rows != 1, 's', '')")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "CALL proc")
	c.Assert(raw.SQL, qt.Not(qt.Contains), "-- end --")
}

func TestParser_ParseMySQLDialectDefinerProcedureAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := `-- atlas:delimiter \n-- end --\n

CREATE DEFINER='boring' PROCEDURE proc ()
    COMMENT 'ATLAS_DELIMITER'
    SQL SECURITY INVOKER
    NOT DETERMINISTIC
    MODIFIES SQL DATA
BEGIN
    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' background thread', IF(@rows != 1, 's', '')) AS summary;
END

-- end --

CALL proc();`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MySQL)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Definer, qt.Equals, "DEFINER='boring'")
	c.Assert(routine.Name, qt.Equals, "proc")
	c.Assert(routine.Characteristics, qt.DeepEquals, []string{
		"COMMENT 'ATLAS_DELIMITER'",
		"SQL SECURITY INVOKER",
		"NOT DETERMINISTIC",
		"MODIFIES SQL DATA",
	})
	c.Assert(routine.SQL, qt.Contains, "CREATE DEFINER='boring' PROCEDURE proc ()")
	c.Assert(routine.SQL, qt.Contains, "SQL SECURITY INVOKER")
	c.Assert(routine.SQL, qt.Contains, "IF(@rows != 1, 's', '')")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CALL proc")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "-- end --")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementSelect,
	})
}

func TestParser_ParseMySQLDialectDefinerFunctionAsRoutine(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE DEFINER=`root`@`localhost` FUNCTION `add2` (a int, b int) RETURNS int DETERMINISTIC RETURN a + b;"
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MySQL)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Definer, qt.Equals, "DEFINER=`root`@`localhost`")
	c.Assert(routine.Name, qt.Equals, "`add2`")
	c.Assert(routine.Parameters, qt.Equals, "a int, b int")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Characteristics, qt.DeepEquals, []string{"DETERMINISTIC"})
	c.Assert(routine.Body.SQL, qt.Equals, "RETURN a + b")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementReturn,
	})
}

func TestParser_ParseMySQLDialectRoutineBodyStatementKinds(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE control_flow()
BEGIN
    DECLARE done, retried BOOL DEFAULT FALSE;
    DECLARE duplicate_key CONDITION FOR SQLSTATE '23000';
    DECLARE cur CURSOR FOR SELECT id FROM users;
    DECLARE CONTINUE HANDLER FOR NOT FOUND, duplicate_key SET done = TRUE;
    SET @seen = 0;
    BEGIN
        SET @seen = @seen + 10;
    END;
    IF @seen = 0 THEN
        SET @seen = 1;
    END IF;
    CASE
        WHEN @seen = 1 THEN SET @seen = 2;
        ELSE SET @seen = 3;
    END CASE;
    WHILE @seen < 5 DO
        SET @seen = @seen + 1;
    END WHILE;
    REPEAT
        SET @seen = @seen - 1;
    UNTIL @seen = 0 END REPEAT;
    scan_loop: LOOP
        LEAVE scan_loop;
    END LOOP scan_loop;
    SELECT IF(@seen = 0, 1, 0);
END;
CREATE TABLE after_proc (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementDeclaration,
		ast.MySQLRoutineStatementDeclaration,
		ast.MySQLRoutineStatementCursor,
		ast.MySQLRoutineStatementHandler,
		ast.MySQLRoutineStatementSet,
		ast.MySQLRoutineStatementBlock,
		ast.MySQLRoutineStatementIf,
		ast.MySQLRoutineStatementCase,
		ast.MySQLRoutineStatementWhile,
		ast.MySQLRoutineStatementRepeat,
		ast.MySQLRoutineStatementLabel,
		ast.MySQLRoutineStatementSelect,
	})
	c.Assert(routine.Body.Statements[0].Declaration, qt.DeepEquals, &ast.MySQLRoutineDeclaration{
		Kind:       ast.MySQLRoutineDeclarationVariable,
		Names:      []string{"done", "retried"},
		TypeSQL:    "BOOL",
		DefaultSQL: "FALSE",
	})
	c.Assert(routine.Body.Statements[1].Declaration, qt.DeepEquals, &ast.MySQLRoutineDeclaration{
		Kind:         ast.MySQLRoutineDeclarationCondition,
		Names:        []string{"duplicate_key"},
		ConditionSQL: "SQLSTATE '23000'",
	})
	c.Assert(routine.Body.Statements[2].Cursor, qt.DeepEquals, &ast.MySQLRoutineCursor{
		Name:      "cur",
		SelectSQL: "SELECT id FROM users",
	})
	c.Assert(routine.Body.Statements[3].Handler, qt.DeepEquals, &ast.MySQLRoutineHandler{
		Action:       "CONTINUE",
		Conditions:   []string{"NOT FOUND", "duplicate_key"},
		StatementSQL: "SET done = TRUE",
	})
	c.Assert(routine.Body.Statements[11].SQL, qt.Contains, "IF(@seen = 0, 1, 0)")

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParseMySQLDialectParenthesizedProceduralIF(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE parenthesized_if()
BEGIN
    DECLARE seen INT DEFAULT 0;
    IF (seen = 0) THEN
        SET seen = 1;
    END IF;
    SELECT IF(seen = 1, 'yes', 'no') AS result;
END;
CREATE TABLE after_proc (id int);`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Body.SQL, qt.Contains, "END IF")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CREATE TABLE after_proc")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementDeclaration,
		ast.MySQLRoutineStatementIf,
		ast.MySQLRoutineStatementSelect,
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParseMariaDBDialectDefinerRoutineHardening(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE OR REPLACE DEFINER=`root`@`localhost` PROCEDURE p1() main: BEGIN\n" +
		"    DECLARE done BOOL DEFAULT FALSE;\n" +
		"    DECLARE cur CURSOR FOR SELECT id FROM users;\n" +
		"    DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN\n" +
		"        SET done = TRUE;\n" +
		"    END;\n" +
		"    OPEN cur;\n" +
		"    FETCH cur INTO done;\n" +
		"    CLOSE cur;\n" +
		"    IF (done = FALSE) THEN\n" +
		"        SET done = TRUE;\n" +
		"    END IF;\n" +
		"    SELECT IF(done, 1, 0) AS value;\n" +
		"END main;\n" +
		"CREATE TABLE after_proc (id int);"
	p := parser.NewParser(sql, parser.WithDialect(platform.MariaDB))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MariaDB)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Definer, qt.Equals, "DEFINER=`root`@`localhost`")
	c.Assert(routine.Name, qt.Equals, "p1")
	c.Assert(routine.SQL, qt.Contains, "CREATE OR REPLACE DEFINER=`root`@`localhost` PROCEDURE p1()")
	c.Assert(routine.SQL, qt.Not(qt.Contains), "CREATE TABLE after_proc")
	c.Assert(routine.Body.SQL, qt.Contains, "main: BEGIN")
	c.Assert(routine.Body.SQL, qt.Contains, "END main")
	c.Assert(routineStatementKinds(routine.Body.Statements), qt.DeepEquals, []ast.MySQLRoutineStatementKind{
		ast.MySQLRoutineStatementDeclaration,
		ast.MySQLRoutineStatementCursor,
		ast.MySQLRoutineStatementHandler,
		ast.MySQLRoutineStatementOpen,
		ast.MySQLRoutineStatementFetch,
		ast.MySQLRoutineStatementClose,
		ast.MySQLRoutineStatementIf,
		ast.MySQLRoutineStatementSelect,
	})
	c.Assert(routine.Body.Statements[2].Handler, qt.DeepEquals, &ast.MySQLRoutineHandler{
		Action:       "CONTINUE",
		Conditions:   []string{"NOT FOUND"},
		StatementSQL: "BEGIN\n        SET done = TRUE;\n    END",
	})

	createTable, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(createTable.Name, qt.Equals, "after_proc")
}

func TestParser_ParseMySQLDialectMalformedDeclareMetadataStaysRaw(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE malformed_handler()
BEGIN
    DECLARE CONTINUE HANDLER FOR;
    DECLARE v INT DEFAULT;
END;`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Body.Statements, qt.HasLen, 2)
	c.Assert(routine.Body.Statements[0].Kind, qt.Equals, ast.MySQLRoutineStatementHandler)
	c.Assert(routine.Body.Statements[0].SQL, qt.Equals, "DECLARE CONTINUE HANDLER FOR;")
	c.Assert(routine.Body.Statements[0].Handler, qt.IsNil)
	c.Assert(routine.Body.Statements[1].Kind, qt.Equals, ast.MySQLRoutineStatementDeclaration)
	c.Assert(routine.Body.Statements[1].SQL, qt.Equals, "DECLARE v INT DEFAULT;")
	c.Assert(routine.Body.Statements[1].Declaration, qt.IsNil)
}

func TestParser_ParseMySQLDialectHandlerMetadataStatementStarts(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE PROCEDURE handler_starts()
BEGIN
    DECLARE v INT DEFAULT 0;
    DECLARE cur CURSOR FOR SELECT id FROM users;
    DECLARE CONTINUE HANDLER FOR NOT FOUND CLOSE cur;
    DECLARE CONTINUE HANDLER FOR SQLSTATE VALUE '23000' FETCH cur INTO v;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 v = MESSAGE_TEXT;
END;`
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.MySQLRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Body.Statements, qt.HasLen, 5)
	c.Assert(routine.Body.Statements[2].Handler, qt.DeepEquals, &ast.MySQLRoutineHandler{
		Action:       "CONTINUE",
		Conditions:   []string{"NOT FOUND"},
		StatementSQL: "CLOSE cur",
	})
	c.Assert(routine.Body.Statements[3].Handler, qt.DeepEquals, &ast.MySQLRoutineHandler{
		Action:       "CONTINUE",
		Conditions:   []string{"SQLSTATE VALUE '23000'"},
		StatementSQL: "FETCH cur INTO v",
	})
	c.Assert(routine.Body.Statements[4].Handler, qt.DeepEquals, &ast.MySQLRoutineHandler{
		Action:       "EXIT",
		Conditions:   []string{"SQLEXCEPTION"},
		StatementSQL: "GET DIAGNOSTICS CONDITION 1 v = MESSAGE_TEXT",
	})
}

func TestParser_ParseMySQLDialectUnsupportedRoutineBodyAsOpaqueRoutine(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE PROCEDURE p1() SELECT 1;"
	p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	routine, ok := statements.Statements[0].(*ast.OpaqueRoutineNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, platform.MySQL)
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.SQL, qt.Equals, "CREATE PROCEDURE p1() SELECT 1")
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

func routineStatementKinds(statements []ast.MySQLRoutineStatement) []ast.MySQLRoutineStatementKind {
	kinds := make([]ast.MySQLRoutineStatementKind, 0, len(statements))
	for _, stmt := range statements {
		kinds = append(kinds, stmt.Kind)
	}
	return kinds
}

func postgresRoutineStatementKinds(statements []ast.PostgresRoutineStatement) []ast.PostgresRoutineStatementKind {
	kinds := make([]ast.PostgresRoutineStatementKind, 0, len(statements))
	for _, stmt := range statements {
		kinds = append(kinds, stmt.Kind)
	}
	return kinds
}

func sqlServerRoutineStatementKinds(statements []ast.SQLServerRoutineStatement) []ast.SQLServerRoutineStatementKind {
	kinds := make([]ast.SQLServerRoutineStatementKind, 0, len(statements))
	for _, stmt := range statements {
		kinds = append(kinds, stmt.Kind)
	}
	return kinds
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

func TestParser_ParseCreateOrReplaceProcedureAsRawSQL(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE OR REPLACE PROCEDURE p() AS BEGIN SELECT 1; END;").Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	raw, ok := statements.Statements[0].(*ast.RawSQLNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(raw.SQL, qt.Contains, "CREATE OR REPLACE PROCEDURE p()")
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

func TestParser_ParseAlterTableAddTypelessColumn(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("ALTER TABLE t ADD COLUMN c;").Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	alterTable, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alterTable.Operations, qt.HasLen, 1)

	addOp, ok := alterTable.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(addOp.Column.Name, qt.Equals, "c")
	c.Assert(addOp.Column.Type, qt.Equals, "")
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
		tables   []string
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
			name:     "multiple tables",
			sql:      "DROP TABLE users, archived_users;",
			table:    "users",
			tables:   []string{"users", "archived_users"},
			rendered: "DROP TABLE users, archived_users;\n",
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
			if tt.tables != nil {
				c.Assert(dropTable.TableNames(), qt.DeepEquals, tt.tables)
			}
			c.Assert(dropTable.IfExists, qt.Equals, tt.ifExists)
			c.Assert(dropTable.Cascade, qt.Equals, tt.cascade)

			rendered, err := renderer.RenderSQL("postgres", dropTable)
			c.Assert(err, qt.IsNil)
			rendered = legacyRenderedSQL(rendered)
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

func TestParser_ParseCreateIndexInclude(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE INDEX idx_users_name ON users (name) INCLUDE (active, version);"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_name")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"name"})
	c.Assert(index.IncludeColumns, qt.DeepEquals, []string{"active", "version"})
}

func TestParser_ParseCreatePartialIndex(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE INDEX idx_users_email_active ON users (email) WHERE deleted_at IS NULL;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email_active")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
	c.Assert(index.Condition, qt.Equals, "deleted_at IS NULL")
}

func TestParser_ParseCreateIndexUsingAndStorageParams(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE INDEX idx_users_c ON users USING BRIN (c) WITH (pages_per_range='2') WHERE c IS NOT NULL;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_c")
	c.Assert(index.Table, qt.Equals, "users")
	c.Assert(index.Type, qt.Equals, "BRIN")
	c.Assert(index.Columns, qt.DeepEquals, []string{"c"})
	c.Assert(index.StorageParams, qt.DeepEquals, map[string]string{"pages_per_range": "2"})
	c.Assert(index.Condition, qt.Equals, "c IS NOT NULL")
}

func TestParser_ParseCreateUniqueIndexNullsNotDistinct(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE UNIQUE INDEX idx_users_c ON users (c) NULLS NOT DISTINCT;"
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Unique, qt.IsTrue)
	c.Assert(index.NullsDistinct, qt.IsNotNil)
	c.Assert(*index.NullsDistinct, qt.IsFalse)
}

func TestParser_ParseCreateIndexIncludeRejectsInvalidLists(t *testing.T) {
	tests := []string{
		"CREATE INDEX idx_users_name ON users (name) INCLUDE ();",
		"CREATE INDEX idx_users_name ON users (name) INCLUDE (active,);",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(sql).Parse()

			c.Assert(err, qt.IsNotNil)
		})
	}
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

	domain, ok := statements.Statements[0].(*ast.CreateTypeNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domain.Name, qt.Equals, "email_domain")
	domainDef, ok := domain.TypeDef.(*ast.DomainTypeDef)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domainDef.BaseType, qt.Equals, "TEXT")
	c.Assert(domainDef.Nullable, qt.IsTrue)
	c.Assert(domainDef.Check, qt.Contains, "VALUE ~*")
}

func TestParser_ParsePostgreSQLQualifiedDomain(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE DOMAIN script_column_domain.positive_int AS bigint CHECK (VALUE > 0);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	domain, ok := statements.Statements[0].(*ast.CreateTypeNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domain.Name, qt.Equals, "script_column_domain.positive_int")
	domainDef, ok := domain.TypeDef.(*ast.DomainTypeDef)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domainDef.BaseType, qt.Equals, "bigint")
	c.Assert(domainDef.Check, qt.Equals, "VALUE > 0")
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
	c.Assert(fullNameCol.GeneratedExpression, qt.Equals, "first_name || ' ' || last_name")
	c.Assert(fullNameCol.GeneratedKind, qt.Equals, "STORED")
	c.Assert(fullNameCol.Check, qt.Equals, "")
}

func TestParser_ParsePostgreSQLIdentityColumns(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
		always_id bigint GENERATED ALWAYS AS IDENTITY (MINVALUE -100 MAXVALUE 10 START WITH 10 INCREMENT BY -2 CACHE 7 CYCLE),
		name character varying NOT NULL
	);`
	p := parser.NewParser(sql)

	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 3)

	idCol := createTable.Columns[0]
	c.Assert(idCol.Name, qt.Equals, "id")
	c.Assert(idCol.Type, qt.Equals, "bigint")
	c.Assert(idCol.Nullable, qt.IsFalse)
	c.Assert(idCol.AutoInc, qt.IsTrue)
	c.Assert(idCol.IdentityGeneration, qt.Equals, "BY_DEFAULT")
	c.Assert(idCol.IdentityStart, qt.Equals, "")
	c.Assert(idCol.IdentityIncrement, qt.Equals, "")
	c.Assert(idCol.IdentityOptions, qt.Equals, "")
	c.Assert(idCol.GeneratedExpression, qt.Equals, "")

	alwaysIDCol := createTable.Columns[1]
	c.Assert(alwaysIDCol.Name, qt.Equals, "always_id")
	c.Assert(alwaysIDCol.AutoInc, qt.IsTrue)
	c.Assert(alwaysIDCol.IdentityGeneration, qt.Equals, "ALWAYS")
	c.Assert(alwaysIDCol.IdentityStart, qt.Equals, "10")
	c.Assert(alwaysIDCol.IdentityIncrement, qt.Equals, "-2")
	c.Assert(alwaysIDCol.IdentityOptions, qt.Equals, "MINVALUE -100 MAXVALUE 10 START WITH 10 INCREMENT BY -2 CACHE 7 CYCLE")
	c.Assert(alwaysIDCol.GeneratedExpression, qt.Equals, "")
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

	// Check domain
	domain, ok := statements.Statements[1].(*ast.CreateTypeNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domain.Name, qt.Equals, "email_domain")
	domainDef, ok := domain.TypeDef.(*ast.DomainTypeDef)
	c.Assert(ok, qt.IsTrue)
	c.Assert(domainDef.BaseType, qt.Equals, "TEXT")
	c.Assert(domainDef.Check, qt.Contains, "VALUE ~*")

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
	c.Assert(fullnameCol.GeneratedExpression, qt.Equals, "CONCAT(`username`, ' ', 'User')")
	c.Assert(fullnameCol.GeneratedKind, qt.Equals, "STORED")

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
	c.Assert(createTable.Columns[1].GeneratedExpression, qt.Equals, "CONCAT(`name`, ' ', 'User')")
	c.Assert(createTable.Columns[1].GeneratedKind, qt.Equals, "STORED")
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

	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].Check, qt.Equals, "`balance` >= 0")
}

func TestParser_ColumnLevelNamedCheckConstraint(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		a int CONSTRAINT c1 CHECK (a > 0),
		b int constraint c2 check (b > 0)
	);`
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 2)

	c.Assert(createTable.Columns[0].Name, qt.Equals, "a")
	c.Assert(createTable.Columns[0].CheckName, qt.Equals, "c1")
	c.Assert(createTable.Columns[0].Check, qt.Equals, "a > 0")

	c.Assert(createTable.Columns[1].Name, qt.Equals, "b")
	c.Assert(createTable.Columns[1].CheckName, qt.Equals, "c2")
	c.Assert(createTable.Columns[1].Check, qt.Equals, "b > 0")
}

func TestParser_TableConstraintColumnParts(t *testing.T) {
	c := qt.New(t)

	sql := "CREATE TABLE t (`id` tinytext NOT NULL, PRIMARY KEY (`id` (7) DESC));"
	statements, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNil)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	pk := createTable.Constraints[0]
	c.Assert(pk.Type, qt.Equals, ast.PrimaryKeyConstraint)
	c.Assert(pk.Columns, qt.DeepEquals, []string{"`id`"})
	c.Assert(pk.ColumnParts, qt.DeepEquals, []ast.ConstraintColumn{{
		Name:   "`id`",
		Prefix: "7",
		Desc:   true,
	}})
}

func TestParser_TablePrimaryKeyInclude(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE users (id integer NOT NULL, covering integer, PRIMARY KEY (id) INCLUDE (covering));`
	statements, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNil)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Constraints, qt.HasLen, 1)

	pk := createTable.Constraints[0]
	c.Assert(pk.Type, qt.Equals, ast.PrimaryKeyConstraint)
	c.Assert(pk.Columns, qt.DeepEquals, []string{"id"})
	c.Assert(pk.IncludeColumns, qt.DeepEquals, []string{"covering"})
}

func TestParser_PrimaryKeyIncludeRejectsInvalidLists(t *testing.T) {
	tests := []string{
		`CREATE TABLE users (id integer NOT NULL, covering integer, PRIMARY KEY (id) INCLUDE ());`,
		`CREATE TABLE users (id integer NOT NULL, covering integer, PRIMARY KEY (id) INCLUDE (covering,));`,
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(sql).Parse()

			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestParser_PrimaryKeyIncludeRejectsColumnConstraint(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(`CREATE TABLE users (id integer PRIMARY KEY INCLUDE (covering), covering integer);`).Parse()

	c.Assert(err, qt.IsNotNil)
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

	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].UpdateExpression, qt.Equals, "CURRENT_TIMESTAMP")
	c.Assert(createTable.Columns[0].Comment, qt.Equals, "")
}

func TestParser_MariaDBOnUpdateBeforeComment(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE test (
		` + "`updated_at`" + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated timestamp'
	);`
	statements, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNil)

	createTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(createTable.Columns, qt.HasLen, 1)
	c.Assert(createTable.Columns[0].UpdateExpression, qt.Equals, "CURRENT_TIMESTAMP")
	c.Assert(createTable.Columns[0].Comment, qt.Equals, "COMMENT 'Updated timestamp'")
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
