package planner_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
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
			fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", test.fixture)
			generated, err := goschema.ParseDir(fixtureDir)
			c.Assert(err, qt.IsNil)
			c.Assert(generated.RLSPolicies, qt.HasLen, test.expectedPolicies)
			c.Assert(generated.RLSEnabledTables, qt.HasLen, test.expectedEnabledTables)
			diff := schemadiff.Compare(generated, &types.DBSchema{})
			sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Not(qt.Equals), "")
			c.Assert(sql, qt.Contains, "CREATE POLICY")
			c.Assert(sql, qt.Contains, "ENABLE ROW LEVEL SECURITY")
		})
	}
}
