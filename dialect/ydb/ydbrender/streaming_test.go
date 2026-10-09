package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbrender"
)

func TestStreamingHandlerRendersWithExplicitTargetFacts(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *ydbast.StreamingQuery
		want  string
	}{
		{"create", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Schema: "jobs.daily", Name: "copy.events", Spec: ast.StreamingQuerySpec{Text: "SELECT 1;", Run: new(false), ResourcePool: "pool"}, Creation: ydbast.StreamingCreation{OrReplace: true, IfNotExists: true}},
			"CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS `jobs.daily/copy.events` WITH (RUN = FALSE, RESOURCE_POOL = `pool`) AS DO BEGIN\nSELECT 1;\nEND DO;"},
		{"alter body", &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Name: "copy", Spec: ast.StreamingQuerySpec{Text: "SELECT 2;"}, Previous: ast.StreamingQuerySpec{Text: "SELECT 1;"}, AllowStateReset: true},
			"ALTER STREAMING QUERY `copy` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\nSELECT 2;\nEND DO;"},
		{"drop", &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Schema: "jobs", Name: "copy"}, "DROP STREAMING QUERY `jobs/copy`;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := renderer.NewExtensions(ydbrender.StreamingHandler())
			c.Assert(err, qt.IsNil)
			caps := capability.YDB262().With(capability.StreamingQueries, true)
			result, err := registry.Render(renderer.ExtensionContext{Target: "ydb", Capabilities: caps}, ast.StatementExtension, test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []string{test.want})
			_, err = registry.Render(renderer.ExtensionContext{Target: "ydb", Capabilities: caps.With(capability.StreamingQueries, false)}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			_, err = registry.Render(renderer.ExtensionContext{Target: "postgres", Capabilities: caps}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		})
	}
}

func TestStreamingHandlerRefusesUnapprovedBodyChange(t *testing.T) {
	c := qt.New(t)
	registry, err := renderer.NewExtensions(ydbrender.StreamingHandler())
	c.Assert(err, qt.IsNil)
	result, err := registry.Render(renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.StreamingQueries, true)}, ast.StatementExtension,
		&ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Name: "copy", Spec: ast.StreamingQuerySpec{Text: "SELECT 2;"}, Previous: ast.StreamingQuerySpec{Text: "SELECT 1;"}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err.Error(), qt.Contains, "allow_state_reset=true")
	c.Assert(result, qt.IsNil)
}
