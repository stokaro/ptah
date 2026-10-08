package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesChangefeedChanges drives a hand-built diff that
// adds a changefeed to a table through every registered planner but YDB's.
// The comparison records the change only where both sides hold changefeeds,
// which only YDB does, and a planner that did not read it would plan nothing
// and report the database synced.
func TestEveryPlannerButYDBRefusesChangefeedChanges(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "users",
		FeatureChanges: []schemaext.ChangeRecord{{
			Subject: ydbschema.ChangefeedRef("", "users", "updates"),
			Value:   &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}}},
		}},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)
			c.Assert(err, qt.ErrorMatches, `(?s).*feature.*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
