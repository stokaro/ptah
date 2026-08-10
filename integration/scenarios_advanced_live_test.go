//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	ptahintegration "go.5x5.cz/ptah/integration"
)

func TestAdvancedScenariosWithRealDatabase(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
	}{
		{name: "dynamic_migration_sql_generation"},
		{name: "dynamic_schema_diff"},
		{name: "dynamic_rollback_to_zero"},
		{name: "migration_generator_validation"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			runner := ptahintegration.NewTestRunner(testFixtures)
			runner.AddDatabase("advanced", requireDynamicDatabaseURL(t))
			runner.AddScenario(requireDynamicScenario(t, test.name))
			c.Assert(runner.RunAll(t.Context()), qt.IsNil)
			report := runner.GetReport()
			c.Assert(report.TotalTests, qt.Equals, 1)
			c.Assert(report.PassedTests, qt.Equals, 1)
			c.Assert(report.FailedTests, qt.Equals, 0)
			c.Assert(report.SkippedTests, qt.Equals, 0)
		})
	}
}
