package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestRLSMigrationGeneration(t *testing.T) {
	c := qt.New(t)

	// Parse the test entities with RLS annotations
	generated, err := goschema.ParseDir("../../integration/fixtures/entities/016-rls-multiple-files")
	c.Assert(err, qt.IsNil)

	// Create an empty database schema (simulating a fresh database)
	dbSchema := &types.DBSchema{
		Tables:      []types.DBTable{},
		RLSPolicies: []types.DBRLSPolicy{},
	}

	// Generate schema diff
	diff := schemadiff.Compare(generated, dbSchema)

	// Generate migration SQL
	sql := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)

	t.Logf("Generated SQL:\n%s", sql)

	// Check that RLS enable statements are generated
	c.Assert(strings.Contains(sql, "ALTER TABLE areas ENABLE ROW LEVEL SECURITY"), qt.IsTrue, qt.Commentf("Missing RLS enable for areas table"))
	c.Assert(strings.Contains(sql, "ALTER TABLE commodities ENABLE ROW LEVEL SECURITY"), qt.IsTrue, qt.Commentf("Missing RLS enable for commodities table"))
	c.Assert(strings.Contains(sql, "ALTER TABLE users ENABLE ROW LEVEL SECURITY"), qt.IsTrue, qt.Commentf("Missing RLS enable for users table"))
	c.Assert(strings.Contains(sql, "ALTER TABLE files ENABLE ROW LEVEL SECURITY"), qt.IsTrue, qt.Commentf("Missing RLS enable for files table"))
	c.Assert(strings.Contains(sql, "ALTER TABLE locations ENABLE ROW LEVEL SECURITY"), qt.IsTrue, qt.Commentf("Missing RLS enable for locations table"))

	// Check that RLS policies are generated
	c.Assert(strings.Contains(sql, "CREATE POLICY area_tenant_isolation ON areas"), qt.IsTrue, qt.Commentf("Missing area RLS policy"))
	c.Assert(strings.Contains(sql, "CREATE POLICY commodity_tenant_isolation ON commodities"), qt.IsTrue, qt.Commentf("Missing commodity RLS policy"))
	c.Assert(strings.Contains(sql, "CREATE POLICY user_tenant_isolation ON users"), qt.IsTrue, qt.Commentf("Missing user RLS policy"))
	c.Assert(strings.Contains(sql, "CREATE POLICY file_tenant_isolation ON files"), qt.IsTrue, qt.Commentf("Missing file RLS policy"))
	c.Assert(strings.Contains(sql, "CREATE POLICY location_tenant_isolation ON locations"), qt.IsTrue, qt.Commentf("Missing location RLS policy"))

	// Check that policies have correct attributes
	c.Assert(strings.Contains(sql, "FOR ALL TO inventario_app"), qt.IsTrue, qt.Commentf("Missing policy attributes"))
	c.Assert(strings.Contains(sql, "USING (tenant_id = get_current_tenant_id())"), qt.IsTrue, qt.Commentf("Missing USING clause"))
}
