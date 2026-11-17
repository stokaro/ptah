//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbtarget"
	ptahintegration "ptah.run/internal/integrationharness"
)

//go:embed internal/fixtures
var embeddedFixtures embed.FS

// testFixtures is the fixture tree in the shape the harness reads: an FS rooted
// where a version directory is `fixtures/entities/<version>`.
//
// The two roots exist because two sources supply the tree. `go:embed` cannot
// name a path outside its own package directory, so the directive has to spell
// the internal one and the extra segment is trimmed back here; the Docker suite
// mounts the same directory at /app/fixtures and hands the harness an FS rooted
// one level further in. Trimming here keeps this caller on the shape it already
// used, so the move changed where the fixtures live and not how they are read.
var testFixtures = must.Must(fs.Sub(embeddedFixtures, "internal"))

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
	runner.AddDatabase("dynamic", scenarioDatabase(c, "ptah_dynamic_scenarios"))
	runner.AddScenario(requireDynamicScenario(t, "dynamic_basic_evolution"))
	runner.AddScenario(requireDynamicScenario(t, "dynamic_idempotency"))
	c.Assert(runner.RunAll(t.Context()), qt.IsNil)
	report := runner.GetReport()
	c.Assert(report.TotalTests, qt.Equals, 2)
	c.Assert(report.PassedTests, qt.Equals, 2)
	c.Assert(report.FailedTests, qt.Equals, 0)
	c.Assert(report.SkippedTests, qt.Equals, 0)
}

func tableNames(schema *schemamodel.Database) map[string]bool {
	names := make(map[string]bool, len(schema.Tables))
	for _, table := range schema.Tables {
		names[table.Name] = true
	}
	return names
}

func enumNames(schema *schemamodel.Database) map[string]bool {
	names := make(map[string]bool, len(schema.Enums))
	for _, enum := range schema.Enums {
		names[enum.Name] = true
	}
	return names
}

// scenarioDatabase creates an empty PostgreSQL database for a harness run and
// drops it when the test ends, returning its address.
//
// The harness empties its database before each scenario and leaves the last
// scenario's tables and revision rows in place, which suits the Docker suite:
// that suite owns its server, and the state left behind is what a reader of a
// failed run inspects. Given the contour's shared database instead, the run
// drops every table the tests before it left there, and leaves its own tables
// and a schema_migrations row for the next test that uses the default revision
// table. That next test then refuses migration 1 on a checksum it never wrote.
func scenarioDatabase(c *qt.C, prefix string) string {
	c.Helper()
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", serverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	return replaceDatabaseName(c, serverURL, name)
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
