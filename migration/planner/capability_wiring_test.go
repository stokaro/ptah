package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
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
		c.Assert(strings.Contains(sql, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;"), qt.IsTrue,
			qt.Commentf("GetPlanner(mariadb) must carry the MariaDB capability preset; got:\n%s", sql))
	})

	t.Run("mysql stays unguarded", func(t *testing.T) {
		c := qt.New(t)

		nodes := planner.GenerateSchemaDiffAST(diff, generated, "mysql")
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;"), qt.IsTrue,
			qt.Commentf("got:\n%s", sql))
		c.Assert(strings.Contains(sql, "IF EXISTS"), qt.IsFalse,
			qt.Commentf("MySQL output must be byte-identical to the pre-capability planner; got:\n%s", sql))
	})
}
