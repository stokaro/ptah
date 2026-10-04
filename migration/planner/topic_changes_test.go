package planner_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesTopicChanges drives a hand-built diff that
// creates, drops and changes a topic through every registered planner but
// YDB's. The comparison feeding those planners refuses a declared topic
// first, so only such a diff reaches them, and a planner that read none of the
// three lists would plan nothing and report the topic applied.
func TestEveryPlannerButYDBRefusesTopicChanges(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	// The planners Ptah ships: a name NormalizeDialect knows. Another test of
	// this package registers planners of its own under names it does not, and
	// those plan whatever their test asks.
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool {
		return dialect == platform.YDB || platform.NormalizeDialect(dialect) != dialect
	})
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	diffs := map[string]*difftypes.SchemaDiff{
		"added":    {TopicsAdded: difftypes.TopicChanges{{Name: "events"}}},
		"removed":  {TopicsRemoved: difftypes.TopicChanges{{Name: "events"}}},
		"modified": {TopicsModified: []difftypes.TopicDiff{{Name: "events", SettingsChanged: true}}},
	}
	for _, dialect := range dialects {
		for name, diff := range diffs {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				nodes, err := planner.GenerateSchemaDiffAST(diff, dialect)
				c.Assert(err, qt.ErrorMatches, `the diff \w+ topic events, which requires target capability topics, .*`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(nodes, qt.IsNil)
			})
		}
	}
}
