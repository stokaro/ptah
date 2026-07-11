package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer"
)

// TestMySQLFamilyRenderers_ConstraintDropGuardValidity pins the renderer-side
// half of the capability model (issue #226) in isolation from any planner:
// given the SAME AST carrying an IF EXISTS intent flag, the mysql renderer
// strips the guard (MySQL rejects it on every constraint-drop spelling) while
// the mariadb renderer honors it.
func TestMySQLFamilyRenderers_ConstraintDropGuardValidity(t *testing.T) {
	dropFK := &ast.AlterTableNode{
		Name: "posts",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "fk_posts_user",
			ForeignKey:     true,
			IfExists:       true,
		}},
	}
	dropCheck := &ast.AlterTableNode{
		Name: "things",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "chk_qty",
			IfExists:       true,
		}},
	}

	t.Run("mysql strips the guard", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQL("mysql", dropFK, dropCheck)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;"), qt.IsTrue,
			qt.Commentf("got:\n%s", sql))
		c.Assert(strings.Contains(sql, "ALTER TABLE things DROP CONSTRAINT chk_qty;"), qt.IsTrue,
			qt.Commentf("got:\n%s", sql))
		c.Assert(strings.Contains(sql, "IF EXISTS"), qt.IsFalse,
			qt.Commentf("MySQL accepts no IF EXISTS on constraint drops; got:\n%s", sql))
	})

	t.Run("mariadb honors the guard", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQL("mariadb", dropFK, dropCheck)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(sql, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;"), qt.IsTrue,
			qt.Commentf("got:\n%s", sql))
		c.Assert(strings.Contains(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_qty;"), qt.IsTrue,
			qt.Commentf("got:\n%s", sql))
	})
}

// TestMySQLFamilyRenderers_DropCheckSpelling pins the dedicated DROP CHECK
// spelling requested via DropConstraintOperation.Check (used by planners for
// MySQL 8.0.16–8.0.18, which lack the generic DROP CONSTRAINT clause) — and
// its validity resolution: MariaDB has no DROP CHECK clause at all (verified
// live on 10.11), so its renderer degrades the request to the generic clause.
func TestMySQLFamilyRenderers_DropCheckSpelling(t *testing.T) {
	c := qt.New(t)

	node := &ast.AlterTableNode{
		Name: "things",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "chk_qty",
			Check:          true,
		}},
	}
	sql, err := renderer.RenderSQL("mysql", node)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(sql, "ALTER TABLE things DROP CHECK chk_qty;"), qt.IsTrue,
		qt.Commentf("got:\n%s", sql))

	sql, err = renderer.RenderSQL("mariadb", node)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(sql, "ALTER TABLE things DROP CONSTRAINT chk_qty;"), qt.IsTrue,
		qt.Commentf("mariadb must degrade DROP CHECK to the generic clause; got:\n%s", sql))
	c.Assert(strings.Contains(sql, "DROP CHECK"), qt.IsFalse,
		qt.Commentf("got:\n%s", sql))
}

// TestMySQLFamilyRenderers_DropUniqueIndexSpelling pins the DROP INDEX
// spelling requested via DropConstraintOperation.Unique (UNIQUE removals on
// targets without the generic clause). ALTER TABLE ... DROP INDEX is valid
// across the entire family, so both renderers honor it as-is.
func TestMySQLFamilyRenderers_DropUniqueIndexSpelling(t *testing.T) {
	c := qt.New(t)

	node := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "uq_email",
			Unique:         true,
		}},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		sql, err := renderer.RenderSQL(dialect, node)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(sql, "ALTER TABLE users DROP INDEX uq_email;"), qt.IsTrue,
			qt.Commentf("%s: got:\n%s", dialect, sql))
	}
}

// TestMySQLFamilyRenderers_DropIndexGuardValidity pins the DROP INDEX guard
// gating: same node, mysql strips IF EXISTS (no such form), mariadb renders
// it (10.1.4+ syntax).
func TestMySQLFamilyRenderers_DropIndexGuardValidity(t *testing.T) {
	c := qt.New(t)

	node := ast.NewDropIndex("idx_users_email").SetIfExists().SetTable("users")

	sqlMySQL, err := renderer.RenderSQL("mysql", node)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(sqlMySQL, "DROP INDEX idx_users_email ON users;"), qt.IsTrue,
		qt.Commentf("got:\n%s", sqlMySQL))
	c.Assert(strings.Contains(sqlMySQL, "IF EXISTS"), qt.IsFalse)

	sqlMariaDB, err := renderer.RenderSQL("mariadb", node)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(sqlMariaDB, "DROP INDEX IF EXISTS idx_users_email ON users;"), qt.IsTrue,
		qt.Commentf("got:\n%s", sqlMariaDB))
}
