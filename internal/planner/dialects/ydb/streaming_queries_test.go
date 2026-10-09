package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestStreamingQueries_StopBeforeSourcesAndRestartAfterCreation(t *testing.T) {
	c := qt.New(t)
	old := &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old.Desired()
	desired.Spec.Text = "INSERT INTO dst SELECT * FROM next;"
	desired.AllowStateReset = true
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbstreaming.Ref("", "copy"), Value: &ydbdiff.StreamingQuery{Before: old, After: desired}}},
		TopicsRemoved:  difftypes.TopicChanges{{Name: "src"}},
		TopicsAdded:    difftypes.TopicChanges{{Name: "next"}},
	}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n"+
		"DROP TOPIC `src`;\nCREATE TOPIC `next`;\n"+
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
