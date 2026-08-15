package main

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/integrationharness"
)

func TestGetStaticScenarios(t *testing.T) {
	c := qt.New(t)

	staticScenarios := getStaticScenarios()
	dynamicScenarios := integrationharness.GetDynamicScenarios()
	allScenarios := integrationharness.GetAllScenarios()

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

		c.Assert(
			scenario.IsRunnable(),
			qt.IsTrue,
			qt.Commentf("Static scenario %s should have a test function", scenario.Name),
		)
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

	dynamicScenarios := integrationharness.GetDynamicScenarios()

	// All dynamic scenarios should have "dynamic_" prefix
	for _, scenario := range dynamicScenarios {
		c.Assert(scenario.Name[:8], qt.Equals, "dynamic_", qt.Commentf("Dynamic scenario %s should have 'dynamic_' prefix", scenario.Name))
	}

	// All dynamic scenarios should have EnhancedTestFunc (based on current implementation)
	for _, scenario := range dynamicScenarios {
		c.Assert(scenario.EnhancedTestFunc, qt.IsNotNil, qt.Commentf("Dynamic scenario %s should have EnhancedTestFunc", scenario.Name))
	}
}

func TestConfiguredDatabaseConnectionsIncludesDistributedSQLAndSQLServer(t *testing.T) {
	c := qt.New(t)

	t.Setenv("POSTGRES_URL", "postgres://postgres.example/db")
	t.Setenv("MYSQL_URL", "mysql://mysql.example/db")
	t.Setenv("MYSQL_CLEANUP_URL", "mysql://root@mysql.example/db")
	t.Setenv("MARIADB_URL", "mariadb://mariadb.example/db")
	t.Setenv("CLICKHOUSE_URL", "clickhouse://clickhouse.example/db")
	t.Setenv("COCKROACHDB_URL", "cockroachdb://cockroach.example/defaultdb")
	t.Setenv("YUGABYTEDB_URL", "yugabytedb://yugabyte.example/yugabyte")
	t.Setenv("SQLSERVER_URL", "sqlserver://sqlserver.example/db")

	connections := configuredDatabaseConnections()

	c.Assert(connections["cockroachdb"], qt.Equals, databaseTarget{
		connectionURL: "cockroachdb://cockroach.example/defaultdb",
		cleanupURL:    "cockroachdb://cockroach.example/defaultdb",
	})
	c.Assert(connections["yugabytedb"], qt.Equals, databaseTarget{
		connectionURL: "yugabytedb://yugabyte.example/yugabyte",
		cleanupURL:    "yugabytedb://yugabyte.example/yugabyte",
	})
	c.Assert(connections["sqlserver"], qt.Equals, databaseTarget{
		connectionURL: "sqlserver://sqlserver.example/db",
		cleanupURL:    "sqlserver://sqlserver.example/db",
	})
	c.Assert(connections["mysql"], qt.Equals, databaseTarget{
		connectionURL: "mysql://mysql.example/db",
		cleanupURL:    "mysql://root@mysql.example/db",
	})
}

func TestRequestedDatabaseConnectionsRejectsMissingRequestedURL(t *testing.T) {
	c := qt.New(t)

	_, err := requestedDatabaseConnections(
		[]string{"postgres", "mysql"},
		map[string]databaseTarget{
			"postgres": {
				connectionURL: "postgres://postgres.example/db",
				cleanupURL:    "postgres://postgres.example/db",
			},
		},
	)

	c.Assert(err, qt.ErrorMatches, `missing database URL for requested database\(s\): mysql`)
}

func TestRequestedDatabaseConnectionsKeepsConfiguredURLs(t *testing.T) {
	c := qt.New(t)

	selected, err := requestedDatabaseConnections(
		[]string{"postgres", "mysql"},
		map[string]databaseTarget{
			"postgres": {
				connectionURL: "postgres://postgres.example/db",
				cleanupURL:    "postgres://postgres.example/db",
			},
			"mysql": {
				connectionURL: "mysql://mysql.example/db",
				cleanupURL:    "mysql://root@mysql.example/db",
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(selected, qt.HasLen, 2)
	c.Assert(selected["postgres"], qt.Equals, databaseTarget{
		connectionURL: "postgres://postgres.example/db",
		cleanupURL:    "postgres://postgres.example/db",
	})
	c.Assert(selected["mysql"], qt.Equals, databaseTarget{
		connectionURL: "mysql://mysql.example/db",
		cleanupURL:    "mysql://root@mysql.example/db",
	})
}

func TestRequestedDatabaseConnectionsNormalizesSQLServerAliases(t *testing.T) {
	c := qt.New(t)

	selected, err := requestedDatabaseConnections(
		[]string{"mssql"},
		map[string]databaseTarget{
			"sqlserver": {
				connectionURL: "sqlserver://sqlserver.example/db",
				cleanupURL:    "sqlserver://sqlserver.example/db",
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(selected, qt.HasLen, 1)
	c.Assert(selected["sqlserver"], qt.Equals, databaseTarget{
		connectionURL: "sqlserver://sqlserver.example/db",
		cleanupURL:    "sqlserver://sqlserver.example/db",
	})
}

func TestDefaultDatabasesIncludeOSSDistributedSQLButNotSQLServer(t *testing.T) {
	c := qt.New(t)

	defaultDatabases, err := newRootCommand().Flags().GetStringSlice(databasesFlag)

	c.Assert(err, qt.IsNil)
	c.Assert(defaultDatabases, qt.Contains, "cockroachdb")
	c.Assert(defaultDatabases, qt.Contains, "yugabytedb")
	c.Assert(defaultDatabases, qt.Not(qt.Contains), "sqlserver")
}
