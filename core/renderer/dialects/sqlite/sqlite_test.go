package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer"
)

func TestRenderCreateTable(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		SetIfNotExists().
		AddColumn(ast.NewColumn("id", "BIGSERIAL").SetPrimary().SetAutoIncrement()).
		AddColumn(ast.NewColumn("email", "VARCHAR(255)").SetNotNull().SetUnique()).
		AddColumn(ast.NewColumn("active", "BOOLEAN").SetDefault("1")).
		AddColumn(ast.NewColumn("status", "ENUM").SetCheck("status IN ('active', 'inactive')")).
		AddConstraint(&ast.ConstraintNode{Type: ast.CheckConstraint, Name: "ck_email", Expression: "length(email) > 3"})
	table.SetOption("STRICT", "true")

	sql, err := renderer.RenderSQL("sqlite", table)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `CREATE TABLE IF NOT EXISTS "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "active" INTEGER DEFAULT '1',
  "status" TEXT CHECK (status IN ('active', 'inactive')),
  CONSTRAINT "ck_email" CHECK (length(email) > 3)
) STRICT;
`)
}

func TestRenderIndexes(t *testing.T) {
	c := qt.New(t)

	idx := ast.NewIndex("idx_users_email", "users", "email").
		SetUnique().
		SetIfNotExists()
	idx.Condition = "email IS NOT NULL"

	sql, err := renderer.RenderSQL("sqlite3", idx)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email") WHERE email IS NOT NULL;
`)

	drop := ast.NewDropIndex("idx_users_email").SetIfExists()
	sql, err = renderer.RenderSQL("sqlite", drop)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP INDEX IF EXISTS \"idx_users_email\";\n")
}

func TestRenderAlterTableNativeSubset(t *testing.T) {
	c := qt.New(t)

	node := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("nickname", "TEXT")},
			&ast.RenameColumnOperation{OldName: "nickname", NewName: "display_name"},
		},
	}

	sql, err := renderer.RenderSQL("sqlite", node)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `ALTER TABLE "users" ADD COLUMN "nickname" TEXT;
ALTER TABLE "users" RENAME COLUMN "nickname" TO "display_name";
`)
}

func TestRenderAlterTableRebuildRequired(t *testing.T) {
	c := qt.New(t)

	node := &ast.AlterTableNode{
		Name:       "users",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: ast.NewColumn("email", "TEXT")}},
	}

	_, err := renderer.RenderSQL("sqlite", node)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `sqlite: *ast.ModifyColumnOperation requires a table rebuild plan`)
}

func TestRenderReplaceTrigger(t *testing.T) {
	c := qt.New(t)

	trigger := ast.NewCreateTrigger("trg_users_ai", "users").
		SetTiming("AFTER").
		SetEvent("INSERT").
		SetBody("BEGIN UPDATE users SET email = NEW.email WHERE id = NEW.id; END;").
		SetReplace()

	sql, err := renderer.RenderSQL("sqlite", trigger)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `DROP TRIGGER IF EXISTS "trg_users_ai";
CREATE TRIGGER "trg_users_ai" AFTER INSERT ON "users" FOR EACH ROW BEGIN UPDATE users SET email = NEW.email WHERE id = NEW.id; END;
`)
}

func TestRenderStatementTriggerRejected(t *testing.T) {
	c := qt.New(t)

	trigger := ast.NewCreateTrigger("trg_users_ai", "users").
		SetTiming("AFTER").
		SetEvent("INSERT").
		SetForEach("STATEMENT").
		SetBody("BEGIN SELECT 1; END")

	_, err := renderer.RenderSQL("sqlite", trigger)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `sqlite: FOR EACH STATEMENT triggers are not supported`)
}

func TestRenderViewWithCheckRejected(t *testing.T) {
	c := qt.New(t)

	view := ast.NewCreateView("active_users").SetBody("SELECT id FROM users").SetWithCheck(true)

	_, err := renderer.RenderSQL("sqlite", view)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `sqlite: WITH CHECK OPTION views are not supported`)
}

func TestRenderAutoIncrementRequiresPrimaryKey(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("bad").
		AddColumn(ast.NewColumn("id", "INTEGER").SetAutoIncrement())

	_, err := renderer.RenderSQL("sqlite", table)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `render column id: unsupported feature: sqlite: AUTOINCREMENT requires an INTEGER PRIMARY KEY column`)
}
