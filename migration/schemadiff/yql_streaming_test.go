package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
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
			held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), StreamingQueries: []catalog.StreamingQuery{{Name: "copy", Spec: ast.StreamingQuerySpec{Text: "SELECT 1;"}}}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
			c.Assert(diff.StreamingQueriesChanged, qt.HasLen, test.changed)
			c.Assert(diff.StreamingQueriesRemoved, qt.HasLen, test.removed)
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
	held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), StreamingQueries: []catalog.StreamingQuery{{Name: "copy", Spec: ast.StreamingQuerySpec{Text: "SELECT 1;"}}}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "ydb", planner.Options{Capabilities: capability.YDB262().With(capability.StreamingQueries, true)},
	)
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	c.Assert(statements, qt.HasLen, 0)
}
