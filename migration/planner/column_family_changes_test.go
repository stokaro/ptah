package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesColumnFamilyChanges drives a hand-built diff
// that puts a column in a YDB column family through every registered planner
// but YDB's. Only the YDB owner plans the change, and a planner that dropped
// it would plan nothing and report the database synced.
func TestEveryPlannerButYDBRefusesColumnFamilyChanges(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "users")
	change := &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"bio"}}}}}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: change}},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)
			c.Assert(err, qt.IsNotNil)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
