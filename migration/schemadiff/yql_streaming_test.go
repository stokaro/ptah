package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestCompare_YQLStreamingQueryOmissionAndReset(t *testing.T) {
	for _, test := range []struct {
		name, source     string
		changed, removed int
	}{
		{name: "declared", source: "CREATE STREAMING QUERY copy AS DO BEGIN SELECT 1; END DO;"},
		{name: "omitted", removed: 1},
		{name: "replacement", source: "CREATE OR REPLACE STREAMING QUERY copy AS DO BEGIN SELECT 2; END DO;", changed: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.source), "ydb")
			c.Assert(err, qt.IsNil)
			held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), FeatureObjects: must.Must(schemaext.NewObjects(ydbstreaming.ObservedObject("", "copy", ydbstreaming.Spec{Text: "SELECT 1;"})))}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), &desired, held, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.StreamingQueries, true)}, nil, must.Must(builtin.New())))
			c.Assert(streamingChangeCounts(c, diff.FeatureChanges), qt.DeepEquals, [2]int{test.changed, test.removed})
			_, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, "ydb", planner.Options{Capabilities: capability.YDB262().With(capability.StreamingQueries, true)},
			)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestCompare_YQLStreamingQueryBodyChangeRequiresPermission(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE STREAMING QUERY copy AS DO BEGIN SELECT 2; END DO;"), "ydb")
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), FeatureObjects: must.Must(schemaext.NewObjects(ydbstreaming.ObservedObject("", "copy", ydbstreaming.Spec{Text: "SELECT 1;"})))}
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), &desired, held, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.StreamingQueries, true)}, nil, must.Must(builtin.New())))
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "ydb", planner.Options{Capabilities: capability.YDB262().With(capability.StreamingQueries, true)},
	)
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	c.Assert(statements, qt.HasLen, 0)
}

func streamingChangeCounts(c *qt.C, changes []schemaext.ChangeRecord) [2]int {
	c.Helper()
	var counts [2]int
	for _, record := range changes {
		c.Assert(record.Subject, qt.Equals, ydbstreaming.Ref("", "copy"))
		change, ok := record.Value.(*ydbdiff.StreamingQuery)
		c.Assert(ok, qt.IsTrue)
		c.Assert(change.Before, qt.IsNotNil)
		if change.After == nil {
			counts[1]++
		} else {
			counts[0]++
		}
	}
	return counts
}
