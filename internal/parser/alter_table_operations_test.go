package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// alterOperationsOf parses one ALTER TABLE under dialect and returns its
// operations.
func alterOperationsOf(c *qt.C, dialect, sql string) []ast.AlterOperation {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	return alter.Operations
}

// Each ALTER TABLE form a schema file may use is read into the operation that
// says what it does. Read as a column definition, ALTER COLUMN a SET DEFAULT 5
// is a column whose type is SET; read as a column drop, DROP CONSTRAINT t_ck
// drops a column named CONSTRAINT.
func TestParse_AlterTableOperations_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		want    []ast.AlterOperation
	}{
		{
			name: "DROP DEFAULT and NOT NULL", dialect: "postgres",
			sql: "ALTER TABLE t ALTER COLUMN a DROP DEFAULT, ALTER COLUMN a DROP NOT NULL, ALTER a SET NOT NULL;",
			want: []ast.AlterOperation{
				&ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnDropDefault},
				&ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnDropNotNull},
				&ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull},
			},
		},
		{
			name: "TYPE and SET DATA TYPE", dialect: "postgres",
			sql: "ALTER TABLE t ALTER COLUMN a TYPE bigint, ALTER COLUMN b SET DATA TYPE varchar(20) USING trim(b, 'x');",
			want: []ast.AlterOperation{
				&ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetType, Type: "bigint"},
				&ast.AlterColumnOperation{
					ColumnName: "b", Action: ast.AlterColumnSetType, Type: "varchar(20)", Using: "trim(b, 'x')",
				},
			},
		},
		{
			name: "DROP COLUMN in each spelling", dialect: "postgres",
			sql: "ALTER TABLE t DROP COLUMN a, DROP b, DROP COLUMN IF EXISTS c RESTRICT, DROP COLUMN d CASCADE;",
			want: []ast.AlterOperation{
				&ast.DropColumnOperation{ColumnName: "a"},
				&ast.DropColumnOperation{ColumnName: "b"},
				&ast.DropColumnOperation{ColumnName: "c", IfExists: true},
				&ast.DropColumnOperation{ColumnName: "d", Cascade: true},
			},
		},
		{
			name: "DROP CONSTRAINT", dialect: "postgres",
			sql: "ALTER TABLE t DROP CONSTRAINT t_ck, DROP CONSTRAINT IF EXISTS t_uq RESTRICT;",
			want: []ast.AlterOperation{
				&ast.DropConstraintOperation{ConstraintName: "t_ck"},
				&ast.DropConstraintOperation{ConstraintName: "t_uq", IfExists: true},
			},
		},
		{
			name: "the MySQL family's DROP spellings", dialect: "mysql",
			sql: "ALTER TABLE t DROP PRIMARY KEY, DROP FOREIGN KEY t_fk, DROP CHECK t_ck, DROP INDEX t_uq, DROP KEY t_k;",
			want: []ast.AlterOperation{
				&ast.DropConstraintOperation{PrimaryKey: true},
				&ast.DropConstraintOperation{ConstraintName: "t_fk", ForeignKey: true},
				&ast.DropConstraintOperation{ConstraintName: "t_ck", Check: true},
				&ast.DropConstraintOperation{ConstraintName: "t_uq", Unique: true},
				&ast.DropConstraintOperation{ConstraintName: "t_k", Unique: true},
			},
		},
		{
			name: "RENAME COLUMN", dialect: "postgres", sql: "ALTER TABLE t RENAME COLUMN a TO b;",
			want: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: "a", NewName: "b"}},
		},
		{
			// PostgreSQL's COLUMN keyword is optional.
			name: "RENAME without COLUMN", dialect: "postgres", sql: "ALTER TABLE t RENAME c TO d;",
			want: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: "c", NewName: "d"}},
		},
		{
			name: "RENAME CONSTRAINT", dialect: "postgres", sql: "ALTER TABLE t RENAME CONSTRAINT x TO y;",
			want: []ast.AlterOperation{&ast.RenameConstraintOperation{From: "x", To: "y"}},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			operations := alterOperationsOf(c, row.dialect, row.sql)

			c.Assert(operations, qt.DeepEquals, row.want)
		})
	}
}

// SQL Server's ALTER COLUMN states a whole new definition, which is MODIFY on
// MySQL, and so does ALTER COLUMN read without a dialect when no action word
// follows the name.
func TestParse_AlterColumnWholeDefinition(t *testing.T) {
	rows := []struct {
		name         string
		dialect      string
		sql          string
		wantType     string
		wantNullable bool
	}{
		{name: "SQL Server", dialect: "sqlserver", sql: "ALTER TABLE t ALTER COLUMN a bigint NOT NULL;", wantType: "bigint"},
		{name: "no dialect", dialect: "", sql: "ALTER TABLE t ALTER COLUMN a bigint;", wantType: "bigint", wantNullable: true},
		{name: "MySQL MODIFY", dialect: "mysql", sql: "ALTER TABLE t MODIFY COLUMN a bigint;", wantType: "bigint", wantNullable: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			operations := alterOperationsOf(c, row.dialect, row.sql)

			c.Assert(operations, qt.HasLen, 1)
			modify, ok := operations[0].(*ast.ModifyColumnOperation)
			c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", operations[0]))
			c.Assert(modify.Column.Name, qt.Equals, "a")
			c.Assert(modify.Column.Type, qt.Equals, row.wantType)
			c.Assert(modify.Column.Nullable, qt.Equals, row.wantNullable)
		})
	}
}

// SET DEFAULT reads the default the way a column declaration does.
func TestParse_AlterColumnSetDefault(t *testing.T) {
	rows := []struct {
		name           string
		sql            string
		wantValue      string
		wantExpression string
	}{
		{name: "a literal", sql: "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5;", wantValue: "5"},
		{name: "a string", sql: "ALTER TABLE t ALTER COLUMN a SET DEFAULT 'x';", wantValue: "'x'"},
		{name: "a function", sql: "ALTER TABLE t ALTER COLUMN a SET DEFAULT now();", wantExpression: "now()"},
		{name: "a function with arguments", sql: "ALTER TABLE t ALTER COLUMN a SET DEFAULT nextval('s'::regclass);", wantExpression: "nextval('s'::regclass)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			operations := alterOperationsOf(c, "postgres", row.sql)

			c.Assert(operations, qt.HasLen, 1)
			alter, ok := operations[0].(*ast.AlterColumnOperation)
			c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", operations[0]))
			c.Assert(alter.Action, qt.Equals, ast.AlterColumnSetDefault)
			c.Assert(alter.Default.Value, qt.Equals, row.wantValue)
			c.Assert(alter.Default.Expression, qt.Equals, row.wantExpression)
		})
	}
}

// What a schema file cannot mean here is refused by name, rather than parsed
// into something that changes nothing.
func TestParse_AlterTableOperations_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name: "an ALTER COLUMN action Ptah does not model", dialect: "postgres",
			sql:     "ALTER TABLE t ALTER COLUMN a SET STATISTICS 100;",
			wantErr: `unsupported ALTER COLUMN a SET STATISTICS at position \d+: a schema file may set a column's default, NOT NULL or type this way; declare anything else on the column itself`,
		},
		{
			name: "identity added later", dialect: "postgres",
			sql:     "ALTER TABLE t ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;",
			wantErr: `unsupported ALTER COLUMN a ADD GENERATED at position \d+: .*`,
		},
		{
			name: "a collation in a type change", dialect: "postgres",
			sql:     `ALTER TABLE t ALTER COLUMN a TYPE text COLLATE "C";`,
			wantErr: `unsupported COLLATE in ALTER COLUMN a TYPE at position \d+: declare the collation on the column itself`,
		},
		{
			name: "USING with nothing after it", dialect: "postgres",
			sql:     "ALTER TABLE t ALTER COLUMN a TYPE bigint USING;",
			wantErr: `expected an expression after USING at position \d+`,
		},
		{
			// PostgreSQL has no ALTER COLUMN a <type>.
			name: "a whole definition under PostgreSQL", dialect: "postgres",
			sql:     "ALTER TABLE t ALTER COLUMN a bigint;",
			wantErr: `expected an ALTER COLUMN a action \(SET, DROP or TYPE\) at position \d+`,
		},
		{
			name: "SQL Server without COLUMN", dialect: "sqlserver",
			sql:     "ALTER TABLE t ALTER a bigint;",
			wantErr: `expected COLUMN after ALTER at position \d+`,
		},
		{
			name: "DROP CONSTRAINT CASCADE", dialect: "postgres",
			sql:     "ALTER TABLE t DROP CONSTRAINT t_ck CASCADE;",
			wantErr: `DROP CONSTRAINT t_ck CASCADE at position \d+: CASCADE also drops the objects that depend on the constraint, which a schema file does not list; drop them by name`,
		},
		{
			name: "DROP FOREIGN without KEY", dialect: "mysql",
			sql:     "ALTER TABLE t DROP FOREIGN t_fk;",
			wantErr: `expected KEY after DROP FOREIGN: .*`,
		},
		{
			name: "IF without EXISTS", dialect: "postgres",
			sql:     "ALTER TABLE t DROP COLUMN IF a;",
			wantErr: `expected EXISTS after IF: .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect(row.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
