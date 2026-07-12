package main

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/integration"
)

func TestGetStaticScenarios(t *testing.T) {
	c := qt.New(t)

	staticScenarios := getStaticScenarios()
	dynamicScenarios := integration.GetDynamicScenarios()
	allScenarios := integration.GetAllScenarios()

	// Static scenarios should not be empty
	c.Assert(len(staticScenarios) > 0, qt.IsTrue, qt.Commentf("Expected static scenarios to exist"))

	// Static + Dynamic should equal All scenarios
	c.Assert(len(staticScenarios)+len(dynamicScenarios), qt.Equals, len(allScenarios))

	// Verify no dynamic scenarios are in static list
	dynamicNames := make(map[string]bool)
	for _, scenario := range dynamicScenarios {
		dynamicNames[scenario.Name] = true
	}

	for _, scenario := range staticScenarios {
		c.Assert(dynamicNames[scenario.Name], qt.IsFalse, qt.Commentf("Static scenario %s should not be in dynamic list", scenario.Name))
	}

	// Verify all static scenarios have required fields
	for _, scenario := range staticScenarios {
		c.Assert(scenario.Name, qt.Not(qt.Equals), "", qt.Commentf("Static scenario name should not be empty"))
		c.Assert(scenario.Description, qt.Not(qt.Equals), "", qt.Commentf("Static scenario description should not be empty"))

		// Should have either TestFunc or EnhancedTestFunc
		hasTestFunc := scenario.TestFunc != nil || scenario.EnhancedTestFunc != nil
		c.Assert(hasTestFunc, qt.IsTrue, qt.Commentf("Static scenario %s should have a test function", scenario.Name))
	}
}

func TestStaticScenarioNaming(t *testing.T) {
	c := qt.New(t)

	staticScenarios := getStaticScenarios()

	// Verify that static scenarios don't have "dynamic_" prefix
	for _, scenario := range staticScenarios {
		c.Assert(scenario.Name[:8], qt.Not(qt.Equals), "dynamic_", qt.Commentf("Static scenario %s should not have 'dynamic_' prefix", scenario.Name))
	}
}

func TestDynamicScenarioIdentification(t *testing.T) {
	c := qt.New(t)

	dynamicScenarios := integration.GetDynamicScenarios()

	// All dynamic scenarios should have "dynamic_" prefix
	for _, scenario := range dynamicScenarios {
		c.Assert(scenario.Name[:8], qt.Equals, "dynamic_", qt.Commentf("Dynamic scenario %s should have 'dynamic_' prefix", scenario.Name))
	}

	// All dynamic scenarios should have EnhancedTestFunc (based on current implementation)
	for _, scenario := range dynamicScenarios {
		c.Assert(scenario.EnhancedTestFunc, qt.IsNotNil, qt.Commentf("Dynamic scenario %s should have EnhancedTestFunc", scenario.Name))
	}
}

func TestConfiguredDatabaseConnectionsIncludesDistributedSQL(t *testing.T) {
	c := qt.New(t)

	t.Setenv("POSTGRES_URL", "postgres://postgres.example/db")
	t.Setenv("MYSQL_URL", "mysql://mysql.example/db")
	t.Setenv("MARIADB_URL", "mariadb://mariadb.example/db")
	t.Setenv("CLICKHOUSE_URL", "clickhouse://clickhouse.example/db")
	t.Setenv("COCKROACHDB_URL", "cockroachdb://cockroach.example/defaultdb")
	t.Setenv("YUGABYTEDB_URL", "yugabytedb://yugabyte.example/yugabyte")

	connections := configuredDatabaseConnections()

	c.Assert(connections["cockroachdb"], qt.Equals, "cockroachdb://cockroach.example/defaultdb")
	c.Assert(connections["yugabytedb"], qt.Equals, "yugabytedb://yugabyte.example/yugabyte")
}

func TestDefaultDatabasesIncludeOSSDistributedSQL(t *testing.T) {
	c := qt.New(t)

	defaultDatabases := rootFlags[databasesFlag].GetStringSlice()

	c.Assert(defaultDatabases, qt.Contains, "cockroachdb")
	c.Assert(defaultDatabases, qt.Contains, "yugabytedb")
}
