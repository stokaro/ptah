package mssql

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
)

func TestRenderer_CreateTableUsesTSQLIdentityAndBrackets(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("dbo.order").
		AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary().SetAutoIncrement()).
		AddColumn(ast.NewColumn("status", "NVARCHAR(255)").SetNotNull().SetDefault("pending")).
		AddColumn(ast.NewColumn("user_id", "INT").SetNotNull()).
		AddConstraint(ast.NewForeignKeyConstraint(
			"fk_order_user",
			[]string{"user_id"},
			&ast.ForeignKeyRef{Table: "dbo.user", Columns: []string{"id"}, OnDelete: "CASCADE"},
		)).
		AddConstraint(&ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       "ck_order_status",
			Expression: "status IN ('pending', 'paid')",
		})

	sql, err := New().Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"CREATE TABLE [dbo].[order] (\n"+
		"  [id] INT IDENTITY(1,1) PRIMARY KEY,\n"+
		"  [status] NVARCHAR(255) NOT NULL DEFAULT 'pending',\n"+
		"  [user_id] INT NOT NULL,\n"+
		"  CONSTRAINT [fk_order_user] FOREIGN KEY ([user_id]) REFERENCES [dbo].[user] ([id]) ON DELETE CASCADE,\n"+
		"  CONSTRAINT [ck_order_status] CHECK (status IN ('pending', 'paid'))\n"+
		");\n")
}

func TestRenderer_IndexAndDropIndexUseSQLServerSyntax(t *testing.T) {
	c := qt.New(t)

	index := ast.NewIndex("idx_order_status", "dbo.order", "status").SetIfNotExists()
	sql, err := New().Render(index)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_order_status' AND object_id = OBJECT_ID('dbo.order'))\n"+
		"CREATE INDEX [idx_order_status] ON [dbo].[order] ([status]);\n")

	drop := ast.NewDropIndex("idx_order_status").SetTable("dbo.order").SetIfExists()
	sql, err = New().Render(drop)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP INDEX IF EXISTS [idx_order_status] ON [dbo].[order];\n")
}

func TestRenderer_AlterTableUsesSQLServerSpelling(t *testing.T) {
	c := qt.New(t)

	alter := &ast.AlterTableNode{
		Name: "dbo.users",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("email", "VARCHAR(320)").SetNotNull()},
			&ast.DropConstraintOperation{ConstraintName: "uk_users_email", Unique: true},
			&ast.ModifyColumnOperation{Column: ast.NewColumn("display_name", "TEXT")},
		},
	}

	sql, err := New().Render(alter)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"ALTER TABLE [dbo].[users] ADD [email] NVARCHAR(320) NOT NULL;\n"+
		"ALTER TABLE [dbo].[users] DROP CONSTRAINT [uk_users_email];\n"+
		"ALTER TABLE [dbo].[users] ALTER COLUMN [display_name] NVARCHAR(MAX) NULL;\n")
}

func TestRenderer_IdentifierEscaping(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewCreateSchema("weird]schema"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE SCHEMA [weird]]schema];\n")
}

func TestRenderer_CreateSchemaIfNotExistsEscapesExecLiteral(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(&ast.CreateSchemaNode{Name: "tenant's]schema", IfNotExists: true})

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"IF SCHEMA_ID('tenant''s]schema') IS NULL\n"+
		"    EXEC('CREATE SCHEMA [tenant''s]]schema]');\n")
}

func TestRenderer_QualifiedBracketIdentifierWithEscapedBracket(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewIndex("idx", "[weird]]schema].[order]", "select"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE INDEX [idx] ON [weird]]schema].[order] ([select]);\n")
}

func TestRenderer_ComputedColumnUsesSQLServerSyntax(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("dbo.invoice").
		AddColumn(ast.NewColumn("subtotal", "INT").SetNotNull()).
		AddColumn(ast.NewColumn("tax", "INT").SetNotNull()).
		AddColumn(ast.NewColumn("total", "INT").SetGenerated("[subtotal] + [tax]", "PERSISTED"))

	sql, err := New().Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "  [total] AS ([subtotal] + [tax]) PERSISTED\n")
	c.Assert(sql, qt.Not(qt.Contains), "[total] INT AS")
}
