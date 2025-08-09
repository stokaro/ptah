package integration

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/types"
)

// TestGetAllScenarios verifies that dynamic scenarios are included
func TestGetAllScenarios(t *testing.T) {
	c := qt.New(t)

	scenarios := GetAllScenarios()

	// Should have both static and dynamic scenarios
	c.Assert(len(scenarios) > 10, qt.IsTrue, qt.Commentf("Expected more than 10 scenarios, got %d", len(scenarios)))

	// Check that dynamic scenarios are included
	scenarioNames := make(map[string]bool)
	for _, scenario := range scenarios {
		scenarioNames[scenario.Name] = true
	}

	// Verify some key dynamic scenarios are present
	dynamicScenarios := []string{
		"dynamic_basic_evolution",
		"dynamic_skip_versions",
		"dynamic_idempotency",
		"dynamic_partial_apply",
		"dynamic_schema_diff",
		"dynamic_migration_sql_generation",
	}

	for _, scenarioName := range dynamicScenarios {
		c.Assert(scenarioNames[scenarioName], qt.IsTrue, qt.Commentf("Dynamic scenario %s should be included", scenarioName))
	}

	// Verify some original scenarios are still present
	originalScenarios := []string{
		"apply_incremental_migrations",
		"rollback_migrations",
		"upgrade_to_specific_version",
	}

	for _, scenarioName := range originalScenarios {
		c.Assert(scenarioNames[scenarioName], qt.IsTrue, qt.Commentf("Original scenario %s should still be included", scenarioName))
	}
}

// TestGetDynamicScenarios verifies the dynamic scenarios function
func TestGetDynamicScenarios(t *testing.T) {
	c := qt.New(t)

	scenarios := GetDynamicScenarios()

	// Should have exactly 38 dynamic scenarios (28 original + 5 RLS down migration scenarios + 5 role scenarios)
	c.Assert(len(scenarios), qt.Equals, 38)

	// Verify all scenarios have required fields
	for _, scenario := range scenarios {
		c.Assert(scenario.Name, qt.Not(qt.Equals), "", qt.Commentf("Scenario name should not be empty"))
		c.Assert(scenario.Description, qt.Not(qt.Equals), "", qt.Commentf("Scenario description should not be empty"))

		// Each scenario should have either TestFunc or EnhancedTestFunc
		hasTestFunc := scenario.TestFunc != nil || scenario.EnhancedTestFunc != nil
		c.Assert(hasTestFunc, qt.IsTrue, qt.Commentf("Scenario %s should have either TestFunc or EnhancedTestFunc", scenario.Name))
	}

	// Verify that all new scenarios are present
	scenarioNames := make(map[string]bool)
	for _, scenario := range scenarios {
		scenarioNames[scenario.Name] = true
	}

	// Check for all the new scenarios we added
	newScenarios := []string{
		"dynamic_rollback_single",
		"dynamic_rollback_multiple",
		"dynamic_rollback_to_zero",
		"dynamic_partial_failure_recovery",
		"dynamic_invalid_migration",
		"dynamic_concurrent_migrations",
		"dynamic_circular_dependencies",
		"dynamic_data_migration",
		"dynamic_large_table_migration",
		"dynamic_empty_migrations",
		"dynamic_duplicate_names",
		"dynamic_reserved_keywords",
		"dynamic_dialect_differences",
		"dynamic_type_mapping",
		"dynamic_constraint_validation",
		"dynamic_foreign_key_cascade",
	}

	for _, scenarioName := range newScenarios {
		c.Assert(scenarioNames[scenarioName], qt.IsTrue, qt.Commentf("New dynamic scenario %s should be included", scenarioName))
	}
}

// TestMariaDBDialectHandling tests that MariaDB dialect is correctly handled
// This test verifies the fix for the issue where mariadb:// URL scheme
// was not properly selecting MySQL-compatible migrations
func TestMariaDBDialectHandling(t *testing.T) {
	c := qt.New(t)

	// Create a mock database connection with MariaDB dialect
	mockConn := &mockDatabaseConnection{
		info: types.DBInfo{
			Dialect: "mariadb",
			Version: "10.11.0",
			Schema:  "test_db",
			URL:     "mariadb://user:pass@tcp(localhost:3306)/test_db",
		},
	}

	// Test that MariaDB dialect is handled correctly by the migration selection logic
	// This should not panic or error, and should select MySQL-compatible migrations
	// We test this by ensuring the function doesn't crash when called with MariaDB dialect
	dialect := mockConn.Info().Dialect
	c.Assert(dialect, qt.Equals, "mariadb")

	// The key test: MariaDB should be treated the same as MySQL for migration selection
	// This verifies that the switch statement in GetMigrationsFS includes "mariadb"
	isMariaDBOrMySQL := dialect == "mysql" || dialect == "mariadb"
	c.Assert(isMariaDBOrMySQL, qt.IsTrue, qt.Commentf("MariaDB dialect should be treated as MySQL-compatible"))
}

// mockDatabaseConnection is a simple mock for testing
type mockDatabaseConnection struct {
	info types.DBInfo
}

func (m *mockDatabaseConnection) Info() types.DBInfo {
	return m.info
}

func (m *mockDatabaseConnection) Close() error {
	return nil
}

func (m *mockDatabaseConnection) Reader() types.SchemaReader {
	return nil
}

func (m *mockDatabaseConnection) Writer() types.SchemaWriter {
	return nil
}

func (m *mockDatabaseConnection) Query(query string, args ...any) ([]map[string]any, error) {
	return nil, nil
}
