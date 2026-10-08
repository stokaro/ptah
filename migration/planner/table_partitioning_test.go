package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesATableSettingsChange drives a diff that changes
// a table's YDB settings through every registered planner but YDB's. Only a
// YDB catalog reports the settings, so such a diff reaches another planner
// from a declaration that names them, and a planner that read nothing of it
// would plan nothing while the comparison kept reporting the difference.
func TestEveryPlannerButYDBRefusesATableSettingsChange(t *testing.T) {
	c := qt.New(t)
	// The dialects Ptah ships, rather than every registered planner: other
	// tests in this package register planners of their own.
	dialects := slices.DeleteFunc(capability.DefaultDialects(), func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("dialects: %v", dialects))

	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "users",
		YDBPartitioningChange: &difftypes.YDBTablePartitioningChange{
			Desired: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(true)},
		},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*the diff changes the partitioning, read replicas or key bloom filter of table "users".*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
