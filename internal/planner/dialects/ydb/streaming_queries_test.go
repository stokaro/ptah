package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestStreamingQueries_StopBeforeSourcesAndRestartAfterCreation(t *testing.T) {
	c := qt.New(t)
	old := &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old.Desired()
	desired.Spec.Text = "INSERT INTO dst SELECT * FROM next;"
	desired.AllowStateReset = true
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbstreaming.Ref("", "copy"), Value: &ydbdiff.StreamingQuery{Before: old, After: desired}},
			{Subject: ydbtopic.Ref("", "src"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			{Subject: ydbtopic.Ref("", "next"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}},
		},
	}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n"+
		"CREATE TOPIC `next`;\nDROP TOPIC `src`;\n"+
		"ALTER STREAMING QUERY `copy` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\nINSERT INTO dst SELECT * FROM next;\nEND DO;\n")
}

func TestStreamingQueries_StoppingNeedsOneStatement(t *testing.T) {
	c := qt.New(t)
	old := &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old.Desired()
	desired.Spec.Run = new(false)
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbstreaming.Ref("", "copy"), Value: &ydbdiff.StreamingQuery{Before: old, After: desired}}}}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n")
}

// TestStreamingQueries_RestartAfterATopicCreatedLast restarts a query after a
// topic the plan creates at the path of a table it drops: the topic follows
// the drop, and the restart follows the topic, which the query reads.
func TestStreamingQueries_RestartAfterATopicCreatedLast(t *testing.T) {
	c := qt.New(t)
	old := &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old.Desired()
	desired.Spec.Text = "INSERT INTO dst SELECT * FROM next;"
	desired.AllowStateReset = true
	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "next", Current: observedFeeds(t, "", "next")}},
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbstreaming.Ref("", "copy"), Value: &ydbdiff.StreamingQuery{Before: old, After: desired}},
			{Subject: ydbtopic.Ref("", "next"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}},
		},
	}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n"+
		"DROP TABLE `next`;\nCREATE TOPIC `next`;\n"+
		"ALTER STREAMING QUERY `copy` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\nINSERT INTO dst SELECT * FROM next;\nEND DO;\n")
}
