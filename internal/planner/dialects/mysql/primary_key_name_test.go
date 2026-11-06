package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
)

// TestAddingAPrimaryKeyWritesNoName is a key the diff names on MySQL, whose primary key is
// always named PRIMARY. A name written in the statement would be one the server drops.
func TestAddingAPrimaryKeyWritesNoName(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
		Name: "t_pk", TableName: "t", Type: "PRIMARY KEY", Columns: []string{"id"},
	}}}

	nodes, err := mysql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(builtin.RenderSQL("mysql", nodes...)), qt.Not(qt.Contains), "t_pk")
}
