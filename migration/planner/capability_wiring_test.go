package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestGetPlanner_CapabilityWiring proves the factory wires the right
// capability preset per dialect (issue #226): the SAME schema diff produces
// guarded constraint drops for mariadb and unguarded ones for mysql, end to
// end through GenerateSchemaDiffAST + the dialect renderer.
func TestGetPlanner_CapabilityWiring(t *testing.T) {
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"fk_posts_user"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "fk_posts_user", TableName: "posts", Type: "FOREIGN KEY"},
		},
	}
	generated := &goschema.Database{}

	t.Run("mariadb gets guarded drops", func(t *testing.T) {
		c := qt.New(t)

		nodes := planner.GenerateSchemaDiffAST(diff, generated, "mariadb")
		sql, err := renderer.RenderSQL("mariadb", nodes...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;",
			qt.Commentf("GetPlanner(mariadb) must carry the MariaDB capability preset; got:\n%s", sql))
	})

	t.Run("mysql stays unguarded", func(t *testing.T) {
		c := qt.New(t)

		nodes := planner.GenerateSchemaDiffAST(diff, generated, "mysql")
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;",
			qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
			qt.Commentf("MySQL output must be byte-identical to the pre-capability planner; got:\n%s", sql))
	})
}

func TestGenerateSchemaDiffSQLStatementsWithCapabilities_UsesServerVersionPreset(t *testing.T) {
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"chk_qty"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "chk_qty", TableName: "things", Type: "CHECK"},
		},
	}
	generated := &goschema.Database{}

	t.Run("mysql 5.7 emits warning instead of invalid drop", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.ForServerVersion("mysql", "5.7.44")

		statements := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, generated, "mysql", caps)
		sql := legacyRenderedSQL(strings.Join(statements, "\n"))

		c.Assert(sql, qt.Contains, "WARNING: cannot drop CHECK constraint chk_qty")
		c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE")
	})

	t.Run("mysql 8.0.17 uses DROP CHECK", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.ForServerVersion("mysql", "8.0.17")

		statements := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, generated, "mysql", caps)
		sql := legacyRenderedSQL(strings.Join(statements, "\n"))

		c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CHECK chk_qty")
	})

	t.Run("mysql 8.0.19 uses generic DROP CONSTRAINT", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.ForServerVersion("mysql", "8.0.19")

		statements := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, generated, "mysql", caps)
		sql := legacyRenderedSQL(strings.Join(statements, "\n"))

		c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT chk_qty")
	})
}

func TestGetPlanner_DistributedSQLCapabilityWiring(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{IndexesAdded: []string{"idx_users_email"}}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
		},
	}

	nodes := planner.GenerateSchemaDiffAST(diff, generated, platform.CockroachDB)
	sql, err := renderer.RenderSQL(platform.CockroachDB, nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);",
		qt.Commentf("got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY",
		qt.Commentf("CockroachDB must stay on plain CREATE INDEX; got:\n%s", sql))
}
