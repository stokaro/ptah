package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestStreamingQueries_StopBeforeSourcesAndRestartAfterCreation(t *testing.T) {
	c := qt.New(t)
	old := schemamodel.StreamingQuery{Name: "copy", Spec: ast.StreamingQuerySpec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old
	desired.Spec.Text = "INSERT INTO dst SELECT * FROM next;"
	desired.AllowStateReset = true
	diff := &difftypes.SchemaDiff{
		StreamingQueriesChanged: []difftypes.StreamingQueryChange{{Current: old, Desired: desired}},
		TopicsRemoved:           difftypes.TopicChanges{{Name: "src"}},
		TopicsAdded:             difftypes.TopicChanges{{Name: "next"}},
	}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n"+
		"DROP TOPIC `src`;\nCREATE TOPIC `next`;\n"+
		"ALTER STREAMING QUERY `copy` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\nINSERT INTO dst SELECT * FROM next;\nEND DO;\n")
}

func TestStreamingQueries_StoppingNeedsOneStatement(t *testing.T) {
	c := qt.New(t)
	old := schemamodel.StreamingQuery{Name: "copy", Spec: ast.StreamingQuerySpec{Text: "INSERT INTO dst SELECT * FROM src;"}}
	desired := old
	desired.Spec.Run = new(false)
	diff := &difftypes.SchemaDiff{StreamingQueriesChanged: []difftypes.StreamingQueryChange{{Current: old, Desired: desired}}}
	got := render(c, capability.YDB262().With(capability.StreamingQueries, true), diff)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `copy` SET (RUN = FALSE, RESOURCE_POOL = `default`);\n")
}
