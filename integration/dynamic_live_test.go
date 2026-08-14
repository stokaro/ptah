//go:build integration

package integration_test

import (
	"embed"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	ptahintegration "go.5x5.cz/ptah/integration"
)

//go:embed fixtures
var testFixtures embed.FS

func TestVersionedEntityManager(t *testing.T) {
	c := qt.New(t)
	manager, err := ptahintegration.NewVersionedEntityManager(testFixtures)
	c.Assert(err, qt.IsNil)
	defer func() {
		c.Assert(manager.Cleanup(), qt.IsNil)
	}()

	t.Run("initial schema", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(manager.LoadEntityVersion("000-initial"), qt.IsNil)
		schema, err := manager.GenerateSchemaFromEntities()
		c.Assert(err, qt.IsNil)
		c.Assert(schema.Tables, qt.HasLen, 2)
		c.Assert(schema.Enums, qt.HasLen, 0)
		c.Assert(tableNames(schema), qt.DeepEquals, map[string]bool{
			"products": true,
			"users":    true,
		})
	})

	t.Run("schema with enums", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(manager.LoadEntityVersion("003-add-enums"), qt.IsNil)
		schema, err := manager.GenerateSchemaFromEntities()
		c.Assert(err, qt.IsNil)
		c.Assert(schema.Tables, qt.HasLen, 3)
		c.Assert(schema.Enums, qt.HasLen, 3)
		c.Assert(enumNames(schema), qt.DeepEquals, map[string]bool{
			"enum_post_status":    true,
			"enum_product_status": true,
			"enum_user_status":    true,
		})
	})
}

func TestDynamicScenariosWithRealDatabase(t *testing.T) {
	c := qt.New(t)
	runner := ptahintegration.NewTestRunner(testFixtures)
	runner.AddDatabase("dynamic", requireDynamicDatabaseURL(t))
	runner.AddScenario(requireDynamicScenario(t, "dynamic_basic_evolution"))
	runner.AddScenario(requireDynamicScenario(t, "dynamic_idempotency"))
	c.Assert(runner.RunAll(t.Context()), qt.IsNil)
	report := runner.GetReport()
	c.Assert(report.TotalTests, qt.Equals, 2)
	c.Assert(report.PassedTests, qt.Equals, 2)
	c.Assert(report.FailedTests, qt.Equals, 0)
	c.Assert(report.SkippedTests, qt.Equals, 0)
}

func tableNames(schema *goschema.Database) map[string]bool {
	names := make(map[string]bool, len(schema.Tables))
	for _, table := range schema.Tables {
		names[table.Name] = true
	}
	return names
}

func enumNames(schema *goschema.Database) map[string]bool {
	names := make(map[string]bool, len(schema.Enums))
	for _, enum := range schema.Enums {
		names[enum.Name] = true
	}
	return names
}

func requireDynamicDatabaseURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_URL", "POSTGRES_URL", "MYSQL_TEST_URL", "MYSQL_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("POSTGRES_TEST_URL, POSTGRES_URL, MYSQL_TEST_URL, or MYSQL_URL is not set")
	return ""
}

func requireDynamicScenario(t *testing.T, name string) ptahintegration.TestScenario {
	t.Helper()
	for _, scenario := range ptahintegration.GetDynamicScenarios() {
		if scenario.Name == name {
			return scenario
		}
	}
	t.Fatalf("dynamic scenario %q is not registered", name)
	return ptahintegration.TestScenario{}
}
