package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesColumnFamilyChanges drives a hand-built diff
// that puts a column in a YDB column family through every registered planner
// but YDB's. Only a YDB catalog reports column families, and a planner that
// did not read the change would plan nothing and report the database synced.
func TestEveryPlannerButYDBRefusesColumnFamilyChanges(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "users",
		YDBColumnFamiliesChange: &difftypes.YDBColumnFamiliesChange{
			Desired: []ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"bio"}}},
		},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)
			c.Assert(err, qt.ErrorMatches, `.*the diff changes the column families of table "users", which only a YDB plan does; .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
