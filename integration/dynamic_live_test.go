//go:build integration

package integration_test

import (
	"embed"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	ptahintegration "go.5x5.cz/ptah/internal/integrationharness"
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

// requireDynamicDatabaseURL returns an address for any engine the dynamic
// scenarios can run against, preferring PostgreSQL, and skips when neither is
// configured.
//
// It uses dbtarget.Lookup rather than dbtarget.URL because the first engine
// being unconfigured is not a reason to skip: the second may still be. A
// lookup that errors is not a miss — it means the variable was set and holds
// another engine's address, and reporting coverage nobody configured is the
// failure this package exists to stop, so it fails the test.
func requireDynamicDatabaseURL(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	for _, engine := range []dbtarget.Engine{dbtarget.PostgreSQL, dbtarget.MySQL} {
		address, err := dbtarget.Lookup(engine)
		c.Assert(err, qt.IsNil)
		if address != "" {
			return address
		}
	}
	t.Skipf(
		"dbtarget: set %s or %s to run the dynamic scenarios against a live database",
		dbtarget.PostgreSQL,
		dbtarget.MySQL,
	)
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
