package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestRLSPolicyGenerationSingleFile(t *testing.T) {
	c := qt.New(t)

	// Test parsing a single file to see if RLS annotations work at all
	database := goschema.ParseFile("../../integration/fixtures/entities/016-rls-multiple-files/area.go")

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

	// We should have at least 1 RLS policy and 1 RLS enabled table from area.go
	c.Assert(len(database.RLSPolicies), qt.Equals, 1, qt.Commentf("Expected 1 RLS policy, got %d", len(database.RLSPolicies)))
	c.Assert(len(database.RLSEnabledTables), qt.Equals, 1, qt.Commentf("Expected 1 RLS enabled table, got %d", len(database.RLSEnabledTables)))
}
