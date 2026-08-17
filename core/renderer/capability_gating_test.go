package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysql"
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
		sql = legacyRenderedSQL(sql)
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;",
			qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT chk_qty;",
			qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
			qt.Commentf("MySQL accepts no IF EXISTS on constraint drops; got:\n%s", sql))
	})

	t.Run("mariadb honors the guard", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQL("mariadb", dropFK, dropCheck)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;",
			qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_qty;",
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
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CHECK chk_qty;",
		qt.Commentf("got:\n%s", sql))

	sql, err = renderer.RenderSQL("mariadb", node)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT chk_qty;",
		qt.Commentf("mariadb must degrade DROP CHECK to the generic clause; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP CHECK",
		qt.Commentf("got:\n%s", sql))
}

func TestMySQLRendererWithCapabilities_UsesPassedCapabilitySet(t *testing.T) {
	c := qt.New(t)

	node := ast.NewDropIndex("idx_users_email").SetIfExists().SetTable("users")

	sql, err := renderer.RenderSQLWithCapabilities("mysql", capability.MySQL84(), node)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "DROP INDEX idx_users_email ON users;")
	c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")

	sql, err = renderer.RenderSQLWithCapabilities("mysql", capability.MariaDB1011(), node)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS idx_users_email ON users;")
}

func TestMySQLRendererWithCapabilities_ClonesCapabilitySet(t *testing.T) {
	c := qt.New(t)

	caps := capability.MySQL84()
	mysqlRenderer := mysql.NewWithCapabilities(caps)
	caps[capability.DropIndexIfExists] = true

	node := ast.NewDropIndex("idx_users_email").SetIfExists().SetTable("users")
	sql, err := mysqlRenderer.Render(node)

	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "DROP INDEX idx_users_email ON users;")
	c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")
}

func TestMySQLFamilyRenderers_IndexPrefixTypes(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect,
				&ast.IndexNode{
					Name:    "idx_users_bio",
					Table:   "users",
					Columns: []string{"bio"},
					Type:    "FULLTEXT",
				},
				&ast.IndexNode{
					Name:    "idx_geom_g",
					Table:   "geom",
					Columns: []string{"g"},
					Type:    "SPATIAL",
				},
			)

			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)
			c.Assert(sql, qt.Contains, "CREATE FULLTEXT INDEX idx_users_bio ON users (bio);",
				qt.Commentf("got:\n%s", sql))
			c.Assert(sql, qt.Contains, "CREATE SPATIAL INDEX idx_geom_g ON geom (g);",
				qt.Commentf("got:\n%s", sql))
		})
	}
}

// TestMySQLFamilyRenderers_DropUniqueIndexSpelling pins the DROP INDEX
// spelling requested via DropConstraintOperation.Unique (every UNIQUE
// removal, issue #195). ALTER TABLE ... DROP INDEX is valid across the entire
// family, so both renderers honor it as-is; the IF EXISTS guard on the
// spelling is MariaDB-only (verified live), so the mysql renderer strips it.
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
		sql = legacyRenderedSQL(sql)
		c.Assert(sql, qt.Contains, "ALTER TABLE users DROP INDEX uq_email;",
			qt.Commentf("%s: got:\n%s", dialect, sql))
	}

	guardedNode := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "uq_email",
			Unique:         true,
			IfExists:       true,
		}},
	}
	sql, err := renderer.RenderSQL("mariadb", guardedNode)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "ALTER TABLE users DROP INDEX IF EXISTS uq_email;",
		qt.Commentf("mariadb honors the guard on the DROP INDEX spelling; got:\n%s", sql))

	sql, err = renderer.RenderSQL("mysql", guardedNode)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "ALTER TABLE users DROP INDEX uq_email;",
		qt.Commentf("mysql strips the guard it cannot parse; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS", qt.Commentf("got:\n%s", sql))
}

// TestMySQLFamilyRenderers_DropIndexGuardValidity pins the DROP INDEX guard
// gating: same node, mysql strips IF EXISTS (no such form), mariadb renders
// it (10.1.4+ syntax).
func TestMySQLFamilyRenderers_DropIndexGuardValidity(t *testing.T) {
	c := qt.New(t)

	node := ast.NewDropIndex("idx_users_email").SetIfExists().SetTable("users")

	sqlMySQL, err := renderer.RenderSQL("mysql", node)
	c.Assert(err, qt.IsNil)
	sqlMySQL = legacyRenderedSQL(sqlMySQL)
	c.Assert(sqlMySQL, qt.Contains, "DROP INDEX idx_users_email ON users;",
		qt.Commentf("got:\n%s", sqlMySQL))
	c.Assert(sqlMySQL, qt.Not(qt.Contains), "IF EXISTS")

	sqlMariaDB, err := renderer.RenderSQL("mariadb", node)
	c.Assert(err, qt.IsNil)
	sqlMariaDB = legacyRenderedSQL(sqlMariaDB)
	c.Assert(sqlMariaDB, qt.Contains, "DROP INDEX IF EXISTS idx_users_email ON users;",
		qt.Commentf("got:\n%s", sqlMariaDB))
}

// TestUnrefinedDialectsStillHonorAPassedCapabilitySet pins the half of the
// capability model that does NOT depend on a dialect having a version ladder.
//
// SQLite and SQL Server renderers take no capabilities of their own
// (`sqlite.New()`, `mssql.New()`), which reads like a set handed to
// NewRendererWithCapabilities is dropped on the floor for them. It is not: the
// constructor validates the set and wraps every dialect, so the model is
// enforced before the inner renderer sees a node. What those two renderers
// cannot yet do is gate their OWN dialect-specific emission on a capability,
// which is a different and still-open half of stokaro/ptah#916.
//
// Without this test the distinction is invisible, and a reader measuring the
// constructors would conclude the wrong thing about both halves.
func TestUnrefinedDialectsStillHonorAPassedCapabilitySet(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite has no version ladder", dialect: "sqlite"},
		{name: "sql server has no version ladder", dialect: "sqlserver"},
		{name: "clickhouse takes capabilities directly", dialect: "clickhouse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			denied := capability.ForDialect(tt.dialect).
				With(capability.ForeignKeysRequireUniqueReference, false).
				With(capability.ForeignKeysCreateBackingIndex, false).
				With(capability.ForeignKeysRequireIndexedReference, false).
				With(capability.ForeignKeys, false)

			r, err := renderer.NewRendererWithCapabilities(tt.dialect, denied)
			c.Assert(err, qt.IsNil)

			err = (&ast.StatementList{
				Statements: []ast.Node{foreignKeyAlterNode("CASCADE", "")},
			}).Accept(r)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tt.dialect+" does not support foreign keys")
			c.Assert(r.Output(), qt.Equals, "")
		})
	}
}

// The control on the test above, and it needs one: without it, "the renderer
// produced nothing" would prove only that this AST never renders on these
// dialects. Turning the capability back ON changes the answer.
//
// SQL Server renders the constraint. SQLite still refuses -- it cannot add a
// constraint without rebuilding the table -- but it refuses for its own reason
// and no longer for the capability's, which is exactly the distinction the
// assertion has to make. A control asserting success here would have been
// wrong about SQLite and would have hidden that.
func TestUnrefinedDialectsRefuseForTheirOwnReasonsWhenTheSetAllows(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantOutput  string
		wantMessage string
	}{
		{
			name:       "sql server renders it",
			dialect:    "sqlserver",
			wantOutput: "fk_children_parents",
		},
		{
			name:        "sqlite refuses it as a table rebuild",
			dialect:     "sqlite",
			wantMessage: "requires a table rebuild plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRendererWithCapabilities(tt.dialect, capability.ForDialect(tt.dialect))
			c.Assert(err, qt.IsNil)

			err = (&ast.StatementList{
				Statements: []ast.Node{foreignKeyAlterNode("CASCADE", "")},
			}).Accept(r)

			c.Assert(errorMessage(err), qt.Contains, tt.wantMessage)
			c.Assert(errorMessage(err), qt.Not(qt.Contains), "does not support foreign keys")
			c.Assert(r.Output(), qt.Contains, tt.wantOutput)
		})
	}
}

// errorMessage is "" for a nil error, so one assertion covers the dialect that
// renders and the dialect that refuses.
func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// A set that contradicts itself is refused at construction rather than carried
// into a render, and the message names the key that has no support under it.
// This is what a caller passing "foreign keys off" on a dialect whose preset
// says foreign keys require a unique reference actually gets.
func TestAnIncoherentCapabilitySetIsRefusedAtConstruction(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite", dialect: "sqlite"},
		{name: "sql server", dialect: "sqlserver"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := renderer.NewRendererWithCapabilities(
				tt.dialect,
				capability.ForDialect(tt.dialect).With(capability.ForeignKeys, false),
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains,
				`capability "foreign_keys_require_unique_reference" requires "foreign_keys"`)
		})
	}
}
