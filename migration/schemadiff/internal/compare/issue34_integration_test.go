package compare

import (
	"testing"

	"github.com/frankban/quicktest"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestIssue34_ExplicitlyDefinedUniqueIndexes tests the specific scenario from GitHub issue #34.
// This test verifies that explicitly defined unique indexes (like tenants_slug_idx and 
// users_tenant_email_idx) are properly detected when they exist in the database and are 
// not regenerated in subsequent migrations.
func TestIssue34_ExplicitlyDefinedUniqueIndexes(t *testing.T) {

	// Test case 1: Initial migration generation - indexes should be added
	t.Run("initial migration - indexes should be added", func(t *testing.T) {
		c := quicktest.New(t)

		// Generated schema has the explicitly defined unique indexes
		generated := &goschema.Database{
			Indexes: []goschema.Index{
				{Name: "tenants_slug_idx"},
				{Name: "users_tenant_email_idx"},
			},
		}

		// Database has no indexes yet (fresh database)
		database := &types.DBSchema{
			Indexes: []types.DBIndex{},
		}

		diff := &difftypes.SchemaDiff{}
		Indexes(generated, database, diff)

		// Both indexes should be added
		c.Assert(diff.IndexesAdded, quicktest.DeepEquals, []string{"tenants_slug_idx", "users_tenant_email_idx"})
		c.Assert(diff.IndexesRemoved, quicktest.DeepEquals, []string(nil))
	})

	// Test case 2: After applying migration - no additional indexes should be generated
	t.Run("after applying migration - no additional indexes should be generated", func(t *testing.T) {
		c := quicktest.New(t)

		// Generated schema still has the same explicitly defined unique indexes
		generated := &goschema.Database{
			Indexes: []goschema.Index{
				{Name: "tenants_slug_idx"},
				{Name: "users_tenant_email_idx"},
			},
		}

		// Database now has the indexes that were created (they are unique indexes)
		database := &types.DBSchema{
			Indexes: []types.DBIndex{
				{Name: "tenants_slug_idx", TableName: "tenants", IsPrimary: false, IsUnique: true},
				{Name: "users_tenant_email_idx", TableName: "users", IsPrimary: false, IsUnique: true},
			},
		}

		diff := &difftypes.SchemaDiff{}
		Indexes(generated, database, diff)

		// No indexes should be added or removed - they already exist and are detected
		c.Assert(diff.IndexesAdded, quicktest.DeepEquals, []string(nil))
		c.Assert(diff.IndexesRemoved, quicktest.DeepEquals, []string(nil))
	})

	// Test case 3: Mixed scenario with constraint-based and explicitly defined indexes
	t.Run("mixed constraint-based and explicitly defined indexes", func(t *testing.T) {
		c := quicktest.New(t)

		// Generated schema has explicitly defined unique indexes
		generated := &goschema.Database{
			Indexes: []goschema.Index{
				{Name: "tenants_slug_idx"},
				{Name: "users_tenant_email_idx"},
			},
		}

		// Database has both constraint-based and explicitly defined indexes
		database := &types.DBSchema{
			Indexes: []types.DBIndex{
				// Constraint-based indexes (should be ignored)
				{Name: "tenants_pkey", TableName: "tenants", IsPrimary: true, IsUnique: false},
				{Name: "users_email_key", TableName: "users", IsPrimary: false, IsUnique: true},
				{Name: "tenants_name_key", TableName: "tenants", IsPrimary: false, IsUnique: true},
				// Explicitly defined indexes (should be compared)
				{Name: "tenants_slug_idx", TableName: "tenants", IsPrimary: false, IsUnique: true},
				{Name: "users_tenant_email_idx", TableName: "users", IsPrimary: false, IsUnique: true},
			},
		}

		diff := &difftypes.SchemaDiff{}
		Indexes(generated, database, diff)

		// No indexes should be added or removed - explicitly defined ones exist, constraint-based ones are ignored
		c.Assert(diff.IndexesAdded, quicktest.DeepEquals, []string(nil))
		c.Assert(diff.IndexesRemoved, quicktest.DeepEquals, []string(nil))
	})

	// Test case 4: One explicitly defined index missing
	t.Run("one explicitly defined index missing", func(t *testing.T) {
		c := quicktest.New(t)

		// Generated schema has both explicitly defined unique indexes
		generated := &goschema.Database{
			Indexes: []goschema.Index{
				{Name: "tenants_slug_idx"},
				{Name: "users_tenant_email_idx"},
			},
		}

		// Database has only one of the explicitly defined indexes
		database := &types.DBSchema{
			Indexes: []types.DBIndex{
				// Constraint-based indexes (should be ignored)
				{Name: "users_email_key", TableName: "users", IsPrimary: false, IsUnique: true},
				// Only one explicitly defined index exists
				{Name: "tenants_slug_idx", TableName: "tenants", IsPrimary: false, IsUnique: true},
				// users_tenant_email_idx is missing
			},
		}

		diff := &difftypes.SchemaDiff{}
		Indexes(generated, database, diff)

		// Only the missing explicitly defined index should be added
		c.Assert(diff.IndexesAdded, quicktest.DeepEquals, []string{"users_tenant_email_idx"})
		c.Assert(diff.IndexesRemoved, quicktest.DeepEquals, []string(nil))
	})
}
