package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
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
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;",
			qt.Commentf("GetPlanner(mariadb) must carry the MariaDB capability preset; got:\n%s", sql))
	})

	t.Run("mysql stays unguarded", func(t *testing.T) {
		c := qt.New(t)

		nodes := planner.GenerateSchemaDiffAST(diff, generated, "mysql")
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;",
			qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
			qt.Commentf("MySQL output must be byte-identical to the pre-capability planner; got:\n%s", sql))
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
	c.Assert(sql, qt.Contains, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);",
		qt.Commentf("got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY",
		qt.Commentf("CockroachDB must stay on plain CREATE INDEX; got:\n%s", sql))
}
