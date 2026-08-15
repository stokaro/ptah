package integrationharness_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/integrationharness"
)

// fixturesRoot is the repository's integration fixture tree, which stays under
// integration/ because the live-database tests read it from there too. The
// harness itself takes the filesystem as a parameter, so this is the only place
// that has to know where the fixtures live relative to this package.
var fixturesRoot = filepath.Join("..", "..", "integration")

func TestGetAllScenariosIncludesStaticAndDynamicScenarios(t *testing.T) {
	c := qt.New(t)
	names := scenarioNames(integrationharness.GetAllScenarios())
	c.Assert(names, qt.Contains, "apply_incremental_migrations")
	c.Assert(names, qt.Contains, "rollback_migrations")
	c.Assert(names, qt.Contains, "upgrade_to_specific_version")
	c.Assert(names, qt.Contains, "dynamic_basic_evolution")
	c.Assert(names, qt.Contains, "dynamic_skip_versions")
	c.Assert(names, qt.Contains, "dynamic_idempotency")
	c.Assert(names, qt.Contains, "dynamic_partial_apply")
	c.Assert(names, qt.Contains, "dynamic_schema_diff")
	c.Assert(names, qt.Contains, "dynamic_migration_sql_generation")
}

func TestGetDynamicScenariosRegistersRunnableMetadata(t *testing.T) {
	c := qt.New(t)
	scenarios := integrationharness.GetDynamicScenarios()
	c.Assert(scenarios, qt.HasLen, 45)

	for _, scenario := range scenarios {
		c.Assert(scenario.Name, qt.Not(qt.Equals), "")
		c.Assert(scenario.Description, qt.Not(qt.Equals), "")
		c.Assert(scenario.IsRunnable(), qt.IsTrue, qt.Commentf("scenario %s must be runnable", scenario.Name))
	}

	names := scenarioNames(scenarios)
	for _, name := range []string{
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
		"dynamic_sqlserver_identity_schema_bracket_reserved_words",
	} {
		c.Assert(names, qt.Contains, name)
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
	addedAccountID := findRoundTripField(c, addedSchema, addedUsers.StructName, "account_id")
	addedManagerID := findRoundTripField(c, addedSchema, addedUsers.StructName, "manager_id")

	c.Assert(baseAccountID.Type, qt.Equals, "INTEGER")
	c.Assert(baseAccountID.Foreign, qt.Equals, "")
	c.Assert(baseManagerID.Type, qt.Equals, "INTEGER")
	c.Assert(baseManagerID.Foreign, qt.Equals, "")
	c.Assert(addedAccountID.Type, qt.Equals, baseAccountID.Type)
	c.Assert(addedAccountID.Foreign, qt.Equals, "accounts(id)")
	c.Assert(addedManagerID.Type, qt.Equals, baseManagerID.Type)
	c.Assert(addedManagerID.Foreign, qt.Equals, "users(id)")
}

func TestSQLServerCompatibleScenariosAreExplicit(t *testing.T) {
	c := qt.New(t)
	expected := map[string]bool{
		"apply_incremental_migrations":  true,
		"rollback_migrations":           true,
		"upgrade_to_specific_version":   true,
		"check_current_version":         true,
		"read_actual_db_schema":         true,
		"dry_run_support":               true,
		"operation_planning":            true,
		"failure_diagnostics":           true,
		"idempotency_reapply":           true,
		"idempotency_up_to_date":        true,
		"parallel_migrate_smoke":        true,
		"cleanup_support":               true,
		"dynamic_circular_dependencies": true,
		"dynamic_sqlserver_identity_schema_bracket_reserved_words": true,
	}
	for _, scenario := range integrationharness.GetAllScenarios() {
		c.Assert(
			scenario.SQLServerCompatible,
			qt.Equals,
			expected[scenario.Name],
			qt.Commentf("unexpected SQL Server compatibility for %s", scenario.Name),
		)
	}
}

func scenarioNames(scenarios []integrationharness.TestScenario) []string {
	names := make([]string, 0, len(scenarios))
	for _, scenario := range scenarios {
		names = append(names, scenario.Name)
	}
	return names
}

func loadRoundTripFixtureSchema(c *qt.C, version string) *goschema.Database {
	c.Helper()
	vem, err := integrationharness.NewVersionedEntityManager(os.DirFS(fixturesRoot))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(vem.Cleanup(), qt.IsNil)
	})
	c.Assert(vem.LoadEntityVersion(version), qt.IsNil)
	schema, err := vem.GenerateSchemaFromEntities()
	c.Assert(err, qt.IsNil)
	return schema
}

func findRoundTripTable(c *qt.C, schema *goschema.Database, name string) *goschema.Table {
	c.Helper()
	index := slices.IndexFunc(schema.Tables, func(table goschema.Table) bool {
		return table.Name == name
	})
	c.Assert(index, qt.Not(qt.Equals), -1, qt.Commentf("missing table %s", name))
	return &schema.Tables[index]
}

func findRoundTripField(c *qt.C, schema *goschema.Database, structName, name string) *goschema.Field {
	c.Helper()
	index := slices.IndexFunc(schema.Fields, func(field goschema.Field) bool {
		return field.StructName == structName && field.Name == name
	})
	c.Assert(index, qt.Not(qt.Equals), -1, qt.Commentf("missing field %s.%s", structName, name))
	return &schema.Fields[index]
}
