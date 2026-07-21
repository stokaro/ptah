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

func TestRenderer_MapsGenericIntegerTypes(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("dbo.metrics").
		AddColumn(ast.NewColumn("small_id", "INTEGER").SetNotNull()).
		AddColumn(ast.NewColumn("legacy_id", "INT4").SetNotNull())

	sql, err := New().Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "  [small_id] INT NOT NULL,\n")
	c.Assert(sql, qt.Contains, "  [legacy_id] INT NOT NULL\n")
}

func TestRenderer_UpsertUsesSQLServerMerge(t *testing.T) {
	c := qt.New(t)

	node := ast.NewUpsert("dbo.user").
		SetInsert([]string{"id", "email", "updated_at"}, []string{"@p1", "@p2", "SYSUTCDATETIME()"}).
		SetMatchColumns("id").
		AddUpdateAssignment("email", "source.[email]").
		AddUpdateAssignment("updated_at", "source.[updated_at]").
		SetComment("upsert user")

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"-- upsert user\n"+
		"MERGE INTO [dbo].[user] WITH (HOLDLOCK) AS target\n"+
		"USING (VALUES (@p1, @p2, SYSUTCDATETIME())) AS source ([id], [email], [updated_at])\n"+
		"ON target.[id] = source.[id]\n"+
		"WHEN MATCHED THEN\n"+
		"    UPDATE SET [email] = source.[email], [updated_at] = source.[updated_at]\n"+
		"WHEN NOT MATCHED THEN\n"+
		"    INSERT ([id], [email], [updated_at])\n"+
		"    VALUES (source.[id], source.[email], source.[updated_at]);\n")
}

func TestRenderer_UpsertEscapesIdentifiersAndRendersPredicates(t *testing.T) {
	c := qt.New(t)

	node := ast.NewUpsert("tenant]db.order").
		SetInsert([]string{"id", "tenant_id", "from"}, []string{"@p1", "@tenant", "@p2"}).
		SetMatchColumns("id", "tenant_id").
		SetUpdatePredicate("target.[deleted_at] IS NULL").
		SetInsertPredicate("@allow_insert = 1").
		AddUpdateAssignment("from", "source.[from]")

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"MERGE INTO [tenant]]db].[order] WITH (HOLDLOCK) AS target\n"+
		"USING (VALUES (@p1, @tenant, @p2)) AS source ([id], [tenant_id], [from])\n"+
		"ON target.[id] = source.[id] AND target.[tenant_id] = source.[tenant_id]\n"+
		"WHEN MATCHED AND (target.[deleted_at] IS NULL) THEN\n"+
		"    UPDATE SET [from] = source.[from]\n"+
		"WHEN NOT MATCHED AND (@allow_insert = 1) THEN\n"+
		"    INSERT ([id], [tenant_id], [from])\n"+
		"    VALUES (source.[id], source.[tenant_id], source.[from]);\n")
}

func TestRenderer_UpsertSanitizesCommentLine(t *testing.T) {
	c := qt.New(t)

	node := ast.NewUpsert("users").
		AddInsertValue("id", "@p1").
		SetMatchColumns("id").
		AddUpdateAssignment("id", "source.[id]").
		SetComment("first line\nDROP TABLE users")

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "-- first line DROP TABLE users\n")
	c.Assert(sql, qt.Not(qt.Contains), "-- first line\nDROP TABLE users")
}

func TestRenderer_UpsertValidation(t *testing.T) {
	tests := []struct {
		name string
		node *ast.UpsertNode
		err  string
	}{
		{
			name: "nil node",
			node: nil,
			err:  "upsert node is nil",
		},
		{
			name: "empty table",
			node: ast.NewUpsert("").
				AddInsertValue("id", "@p1").
				SetMatchColumns("id").
				AddUpdateAssignment("id", "source.[id]"),
			err: "upsert table is required",
		},
		{
			name: "mismatched insert values",
			node: ast.NewUpsert("users").
				SetInsert([]string{"id"}, []string{"@p1", "@p2"}).
				SetMatchColumns("id").
				AddUpdateAssignment("id", "source.[id]"),
			err: `upsert insert columns and values length mismatch: 1 columns, 2 values`,
		},
		{
			name: "empty insert column",
			node: ast.NewUpsert("users").
				AddInsertValue(" ", "@p1").
				SetMatchColumns("id").
				AddUpdateAssignment("id", "source.[id]"),
			err: "upsert insert column is empty",
		},
		{
			name: "empty value expression",
			node: ast.NewUpsert("users").
				AddInsertValue("id", " ").
				SetMatchColumns("id").
				AddUpdateAssignment("id", "source.[id]"),
			err: "upsert value expression is empty",
		},
		{
			name: "duplicate insert column",
			node: ast.NewUpsert("users").
				SetInsert([]string{"id", "[id]"}, []string{"@p1", "@p2"}).
				SetMatchColumns("id").
				AddUpdateAssignment("id", "source.[id]"),
			err: `upsert insert column "\[id\]" is duplicated`,
		},
		{
			name: "empty match column",
			node: ast.NewUpsert("users").
				AddInsertValue("id", "@p1").
				SetMatchColumns(" ").
				AddUpdateAssignment("id", "source.[id]"),
			err: "upsert match column is empty",
		},
		{
			name: "duplicate match column",
			node: ast.NewUpsert("users").
				AddInsertValue("id", "@p1").
				SetMatchColumns("id", "[id]").
				AddUpdateAssignment("id", "source.[id]"),
			err: `upsert match column "\[id\]" is duplicated`,
		},
		{
			name: "match column missing from insert columns",
			node: ast.NewUpsert("users").
				AddInsertValue("email", "@p1").
				SetMatchColumns("id").
				AddUpdateAssignment("email", "source.[email]"),
			err: `upsert match column "id" must also be an insert column`,
		},
		{
			name: "empty update expression",
			node: ast.NewUpsert("users").
				AddInsertValue("id", "@p1").
				SetMatchColumns("id").
				AddUpdateAssignment("email", " "),
			err: "upsert update assignment expression is empty",
		},
		{
			name: "duplicate update assignment column",
			node: ast.NewUpsert("users").
				AddInsertValue("id", "@p1").
				SetMatchColumns("id").
				AddUpdateAssignment("email", "source.[email]").
				AddUpdateAssignment("[email]", "source.[email]"),
			err: `upsert update assignment column "\[email\]" is duplicated`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := New().Render(tt.node)

			c.Assert(sql, qt.Equals, "")
			c.Assert(err, qt.ErrorMatches, tt.err)
		})
	}
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
