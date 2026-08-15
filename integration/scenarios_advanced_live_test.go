//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	ptahintegration "go.5x5.cz/ptah/internal/integrationharness"
)

func TestAdvancedScenariosWithRealDatabase(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "dynamic_migration_sql_generation"},
		{name: "dynamic_schema_diff"},
		{name: "dynamic_rollback_to_zero"},
		{name: "migration_generator_validation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			runner := ptahintegration.NewTestRunner(testFixtures)
			runner.AddDatabase("advanced", requireDynamicDatabaseURL(t))
			runner.AddScenario(requireAdvancedScenario(t, test.name))
			c.Assert(runner.RunAll(t.Context()), qt.IsNil)
			report := runner.GetReport()
			c.Assert(report.TotalTests, qt.Equals, 1)
			c.Assert(report.PassedTests, qt.Equals, 1)
			c.Assert(report.FailedTests, qt.Equals, 0)
			c.Assert(report.SkippedTests, qt.Equals, 0)
		})
	}
}

func requireAdvancedScenario(t *testing.T, name string) ptahintegration.TestScenario {
	t.Helper()

	for _, scenario := range ptahintegration.GetAllScenarios() {
		if scenario.Name == name {
			return scenario
		}
	}
	t.Fatalf("advanced scenario %q is not registered", name)
	return ptahintegration.TestScenario{}
}
