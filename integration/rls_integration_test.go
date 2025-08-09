package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestRLSIntegrationFixtures(t *testing.T) {
	testCases := []struct {
		name                  string
		fixtureDir            string
		expectedPolicies      int
		expectedEnabledTables int
		description           string
	}{
		{
			name:                  "RLS Functions Fixture",
			fixtureDir:            "fixtures/entities/014-rls-functions",
			expectedPolicies:      2,
			expectedEnabledTables: 2,
			description:           "Basic RLS with functions should work",
		},
		{
			name:                  "RLS Advanced Fixture",
			fixtureDir:            "fixtures/entities/015-rls-advanced",
			expectedPolicies:      4,
			expectedEnabledTables: 2,
			description:           "Advanced RLS with multiple policies should work",
		},
		{
			name:                  "RLS Multiple Files Fixture (Fixed)",
			fixtureDir:            "fixtures/entities/016-rls-multiple-files",
			expectedPolicies:      5,
			expectedEnabledTables: 5,
			description:           "Multiple files with RLS should now work after the fix",
		},
		{
			name:                  "RLS Inventario Reproduction (Fixed)",
			fixtureDir:            "fixtures/entities/017-rls-inventario-reproduction",
			expectedPolicies:      5,
			expectedEnabledTables: 5,
			description:           "Inventario reproduction case should now work after the fix",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			// Parse the fixture directory
			generated, err := goschema.ParseDir(tc.fixtureDir)
			c.Assert(err, qt.IsNil, qt.Commentf("Failed to parse fixture directory: %s", tc.fixtureDir))

			// Check that the expected number of RLS policies are parsed
			c.Assert(len(generated.RLSPolicies), qt.Equals, tc.expectedPolicies,
				qt.Commentf("Expected %d RLS policies, got %d. %s", tc.expectedPolicies, len(generated.RLSPolicies), tc.description))

			// Check that the expected number of RLS enabled tables are parsed
			c.Assert(len(generated.RLSEnabledTables), qt.Equals, tc.expectedEnabledTables,
				qt.Commentf("Expected %d RLS enabled tables, got %d. %s", tc.expectedEnabledTables, len(generated.RLSEnabledTables), tc.description))

			// Test migration generation
			dbSchema := &types.DBSchema{
				Tables:      []types.DBTable{},
				RLSPolicies: []types.DBRLSPolicy{},
			}

			diff := schemadiff.Compare(generated, dbSchema)
			sql := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)

			// Verify that SQL is generated (non-empty)
			c.Assert(len(sql) > 0, qt.IsTrue, qt.Commentf("No SQL generated for fixture: %s", tc.fixtureDir))

			// Log the generated SQL for debugging
			t.Logf("Generated SQL for %s:\n%s", tc.name, sql)

			// Verify that RLS policies are in the generated SQL
			if tc.expectedPolicies > 0 {
				c.Assert(sql, qt.Contains, "CREATE POLICY", qt.Commentf("Generated SQL should contain CREATE POLICY statements"))
			}

			// Verify that RLS enable statements are in the generated SQL
			if tc.expectedEnabledTables > 0 {
				c.Assert(sql, qt.Contains, "ENABLE ROW LEVEL SECURITY", qt.Commentf("Generated SQL should contain ENABLE ROW LEVEL SECURITY statements"))
			}
		})
	}
}
