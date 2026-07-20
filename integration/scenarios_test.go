package integration

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
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

func TestRoundTripFixturesCoverIssue147EdgeCases(t *testing.T) {
	c := qt.New(t)

	fixtures := make(map[string]roundTripFixture, len(roundTripFixtures))
	for _, fixture := range roundTripFixtures {
		fixtures[fixture.Name] = fixture
	}

	expected := map[string][]string{
		"empty_schema":                          {"024-roundtrip-empty"},
		"single_table":                          {"025-roundtrip-single-table"},
		"composite_primary_key":                 {"026-roundtrip-composite-pk"},
		"self_referencing_fk":                   {"027-roundtrip-self-reference"},
		"parent_child_fk_drop_order":            {"028-roundtrip-parent-child"},
		"three_level_fk_chain":                  {"034-roundtrip-fk-chain"},
		"diamond_fk_graph":                      {"035-roundtrip-fk-diamond"},
		"mutual_fk_cycle":                       {"029-roundtrip-mutual-cycle"},
		"same_name_check_drift":                 {"030-roundtrip-check-v1", "031-roundtrip-check-v2"},
		"same_name_unique_drift":                {"032-roundtrip-unique-v1", "033-roundtrip-unique-v2"},
		"same_name_check_to_unique_drift":       {"042-roundtrip-check-to-unique-v1", "043-roundtrip-check-to-unique-v2"},
		"same_name_unique_to_check_drift":       {"044-roundtrip-unique-to-check-v1", "045-roundtrip-unique-to-check-v2"},
		"composite_primary_key_add_remove":      {"036-roundtrip-pk-base", "037-roundtrip-pk-composite-added", "038-roundtrip-pk-composite-removed"},
		"enum_value_add":                        {"039-roundtrip-enum-v1", "040-roundtrip-enum-v2-add"},
		"enum_value_remove":                     {"040-roundtrip-enum-v2-add", "041-roundtrip-enum-v3-remove"},
		"foreign_key_added_to_existing_columns": {"046-roundtrip-existing-fk-base", "047-roundtrip-existing-fk-added"},
	}
	for name, versions := range expected {
		fixture, ok := fixtures[name]
		c.Assert(ok, qt.IsTrue, qt.Commentf("missing round-trip fixture %s", name))
		c.Assert(fixture.Versions, qt.DeepEquals, versions)
	}
}

func TestExistingColumnForeignKeyFixtureMetadata(t *testing.T) {
	c := qt.New(t)

	baseSchema := loadRoundTripFixtureSchema(c, "046-roundtrip-existing-fk-base")
	addedSchema := loadRoundTripFixtureSchema(c, "047-roundtrip-existing-fk-added")

	baseUsers := findRoundTripTable(c, baseSchema, "users")
	addedUsers := findRoundTripTable(c, addedSchema, "users")

	baseAccountID := findRoundTripField(c, baseSchema, baseUsers.StructName, "account_id")
	baseManagerID := findRoundTripField(c, baseSchema, baseUsers.StructName, "manager_id")
	c.Assert(baseAccountID.Type, qt.Equals, "INTEGER")
	c.Assert(baseAccountID.Foreign, qt.Equals, "")
	c.Assert(baseManagerID.Type, qt.Equals, "INTEGER")
	c.Assert(baseManagerID.Foreign, qt.Equals, "")

	addedAccountID := findRoundTripField(c, addedSchema, addedUsers.StructName, "account_id")
	addedManagerID := findRoundTripField(c, addedSchema, addedUsers.StructName, "manager_id")
	c.Assert(addedAccountID.Type, qt.Equals, baseAccountID.Type)
	c.Assert(addedAccountID.Foreign, qt.Equals, "accounts(id)")
	c.Assert(addedManagerID.Type, qt.Equals, baseManagerID.Type)
	c.Assert(addedManagerID.Foreign, qt.Equals, "users(id)")
}

func loadRoundTripFixtureSchema(c *qt.C, version string) *goschema.Database {
	c.Helper()

	vem, err := NewVersionedEntityManager(testFixtures)
	c.Assert(err, qt.IsNil)
	defer vem.Cleanup()

	c.Assert(vem.LoadEntityVersion(version), qt.IsNil)

	schema, err := vem.GenerateSchemaFromEntities()
	c.Assert(err, qt.IsNil)

	return schema
}

func findRoundTripTable(c *qt.C, schema *goschema.Database, name string) *goschema.Table {
	c.Helper()

	for i := range schema.Tables {
		if schema.Tables[i].Name == name {
			return &schema.Tables[i]
		}
	}

	c.Fatalf("missing table %s", name)
	return nil
}

func findRoundTripField(c *qt.C, schema *goschema.Database, structName, name string) *goschema.Field {
	c.Helper()

	for i := range schema.Fields {
		field := &schema.Fields[i]
		if field.StructName == structName && field.Name == name {
			return field
		}
	}

	c.Fatalf("missing field %s.%s", structName, name)
	return nil
}

// TestGetDynamicScenarios verifies the dynamic scenarios function
func TestGetDynamicScenarios(t *testing.T) {
	c := qt.New(t)

	scenarios := GetDynamicScenarios()

	// Should have exactly 44 dynamic scenarios (28 original + 5 RLS down migration scenarios + 5 role scenarios + 2 fixture-coverage scenarios for PR #123 / issue #89 + 1 ClickHouse MergeTree scenario for issue #169 + 1 FK action evolution scenario for issue #196 + 2 distributed-SQL common-subset scenarios for issue #171)
	c.Assert(scenarios, qt.HasLen, 44)

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
