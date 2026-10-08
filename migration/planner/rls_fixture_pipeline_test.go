package planner_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestRLSFixturePipeline(t *testing.T) {
	tests := []struct {
		name                  string
		fixture               string
		expectedPolicies      int
		expectedEnabledTables int
	}{
		{name: "functions", fixture: "014-rls-functions", expectedPolicies: 2, expectedEnabledTables: 2},
		{name: "advanced", fixture: "015-rls-advanced", expectedPolicies: 4, expectedEnabledTables: 2},
		{name: "multiple files", fixture: "016-rls-multiple-files", expectedPolicies: 5, expectedEnabledTables: 5},
		{name: "inventario reproduction", fixture: "017-rls-inventario-reproduction", expectedPolicies: 5, expectedEnabledTables: 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixtureDir := filepath.Join("..", "..", "integration", "internal", "fixtures", "entities", test.fixture)
			desired, err := goschema.ParseDir(fixtureDir)
			c.Assert(err, qt.IsNil)
			c.Assert(desired.RLSPolicies, qt.HasLen, test.expectedPolicies)
			c.Assert(desired.RLSEnabledTables, qt.HasLen, test.expectedEnabledTables)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, platform.Postgres, must.Must(builtin.New())))
			sql, err := planner.GenerateSchemaDiffSQL(
				context.Background(), must.Must(builtin.New()),
				diff, platform.Postgres,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Not(qt.Equals), "")
			c.Assert(sql, qt.Contains, "CREATE POLICY")
			c.Assert(sql, qt.Contains, "ENABLE ROW LEVEL SECURITY")
		})
	}
}
