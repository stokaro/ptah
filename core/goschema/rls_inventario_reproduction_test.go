package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestRLSPolicyGenerationInventarioReproduction(t *testing.T) {
	c := qt.New(t)

	// Parse the directory that reproduces the exact inventario project structure
	database, err := goschema.ParseDir("../../integration/fixtures/entities/017-rls-inventario-reproduction")
	c.Assert(err, qt.IsNil)

	// Debug output
	t.Logf("Found %d tables", len(database.Tables))
	for _, table := range database.Tables {
		t.Logf("Table: %s", table.Name)
	}

	t.Logf("Found %d RLS policies", len(database.RLSPolicies))
	for _, policy := range database.RLSPolicies {
		t.Logf("RLS Policy: %s for table %s", policy.Name, policy.Table)
	}

	t.Logf("Found %d RLS enabled tables", len(database.RLSEnabledTables))
	for _, table := range database.RLSEnabledTables {
		t.Logf("RLS Enabled Table: %s", table.Table)
	}

	// We should have 5 RLS policies (one from each file)
	expectedPolicies := []string{
		"area_tenant_isolation",
		"commodity_tenant_isolation",
		"user_tenant_isolation",
		"file_tenant_isolation",
		"location_tenant_isolation",
	}

	c.Assert(len(database.RLSPolicies), qt.Equals, len(expectedPolicies), qt.Commentf("Expected %d RLS policies, got %d", len(expectedPolicies), len(database.RLSPolicies)))

	// Check that all expected policies are present
	policyNames := make(map[string]bool)
	for _, policy := range database.RLSPolicies {
		policyNames[policy.Name] = true
	}

	for _, expectedPolicy := range expectedPolicies {
		c.Assert(policyNames[expectedPolicy], qt.IsTrue, qt.Commentf("Expected policy %s not found", expectedPolicy))
	}

	// We should also have 5 RLS enabled tables
	expectedTables := []string{
		"areas",
		"commodities",
		"users",
		"files",
		"locations",
	}

	c.Assert(len(database.RLSEnabledTables), qt.Equals, len(expectedTables), qt.Commentf("Expected %d RLS enabled tables, got %d", len(expectedTables), len(database.RLSEnabledTables)))

	// Check that all expected tables are present
	tableNames := make(map[string]bool)
	for _, table := range database.RLSEnabledTables {
		tableNames[table.Table] = true
	}

	for _, expectedTable := range expectedTables {
		c.Assert(tableNames[expectedTable], qt.IsTrue, qt.Commentf("Expected RLS enabled table %s not found", expectedTable))
	}
}
