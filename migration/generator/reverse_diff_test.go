package generator

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestReverseSchemaDiff_Extensions(t *testing.T) {
	tests := []struct {
		name     string
		input    *types.SchemaDiff
		expected *types.SchemaDiff
	}{
		{
			name: "reverse extensions added and removed",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{"pg_trgm", "btree_gin"},
				ExtensionsRemoved: []string{"postgis", "uuid-ossp"},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{"postgis", "uuid-ossp"},
				ExtensionsRemoved: []string{"pg_trgm", "btree_gin"},
			},
		},
		{
			name: "reverse only extensions added",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{"pg_trgm"},
				ExtensionsRemoved: []string{},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{"pg_trgm"},
			},
		},
		{
			name: "reverse only extensions removed",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{"postgis"},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{"postgis"},
				ExtensionsRemoved: []string{},
			},
		},
		{
			name: "no extensions to reverse",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{},
			},
		},
		{
			name: "nil extension slices",
			input: &types.SchemaDiff{
				ExtensionsAdded:   nil,
				ExtensionsRemoved: nil,
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   nil,
				ExtensionsRemoved: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := reverseSchemaDiff(tt.input)

			c.Assert(result.ExtensionsAdded, qt.DeepEquals, tt.expected.ExtensionsAdded)
			c.Assert(result.ExtensionsRemoved, qt.DeepEquals, tt.expected.ExtensionsRemoved)
		})
	}
}

func TestReverseSchemaDiff_CompleteReversal(t *testing.T) {
	c := qt.New(t)

	// Test that all fields are properly reversed
	input := &types.SchemaDiff{
		TablesAdded:       []string{"users", "posts"},
		TablesRemoved:     []string{"old_table"},
		EnumsAdded:        []string{"status_type"},
		EnumsRemoved:      []string{"old_enum"},
		IndexesAdded:      []string{"idx_users_email"},
		IndexesRemoved:    []string{"idx_old"},
		ExtensionsAdded:   []string{"pg_trgm", "btree_gin"},
		ExtensionsRemoved: []string{"postgis"},
		FunctionsAdded:    []string{"get_tenant_id", "set_tenant_context"},
		FunctionsRemoved:  []string{"old_function"},
		RLSPoliciesAdded:  []string{"user_policy", "tenant_policy"},
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "old_policy", TableName: "old_table"},
		},
		RLSEnabledTablesAdded:   []string{"users", "posts"},
		RLSEnabledTablesRemoved: []string{"old_table"},
		RolesAdded:              []string{"app_user", "admin_user"},
		RolesRemoved:            []string{"old_role"},
	}

	result := reverseSchemaDiff(input)

	// Verify all reversals
	c.Assert(result.TablesAdded, qt.DeepEquals, input.TablesRemoved)
	c.Assert(result.TablesRemoved, qt.DeepEquals, input.TablesAdded)
	c.Assert(result.EnumsAdded, qt.DeepEquals, input.EnumsRemoved)
	c.Assert(result.EnumsRemoved, qt.DeepEquals, input.EnumsAdded)
	c.Assert(result.IndexesAdded, qt.DeepEquals, input.IndexesRemoved)
	c.Assert(result.IndexesRemoved, qt.DeepEquals, input.IndexesAdded)
	c.Assert(result.ExtensionsAdded, qt.DeepEquals, input.ExtensionsRemoved)
	c.Assert(result.ExtensionsRemoved, qt.DeepEquals, input.ExtensionsAdded)

	// Verify function reversals
	c.Assert(result.FunctionsAdded, qt.DeepEquals, input.FunctionsRemoved)
	c.Assert(result.FunctionsRemoved, qt.DeepEquals, input.FunctionsAdded)

	// Verify RLS policy reversals
	expectedRLSPoliciesAdded := []string{"old_policy"}
	c.Assert(result.RLSPoliciesAdded, qt.DeepEquals, expectedRLSPoliciesAdded)

	expectedRLSPoliciesRemoved := []types.RLSPolicyRef{
		{PolicyName: "user_policy", TableName: ""},
		{PolicyName: "tenant_policy", TableName: ""},
	}
	c.Assert(result.RLSPoliciesRemoved, qt.DeepEquals, expectedRLSPoliciesRemoved)

	// Verify RLS table enablement reversals
	c.Assert(result.RLSEnabledTablesAdded, qt.DeepEquals, input.RLSEnabledTablesRemoved)
	c.Assert(result.RLSEnabledTablesRemoved, qt.DeepEquals, input.RLSEnabledTablesAdded)

	// Verify role reversals
	c.Assert(result.RolesAdded, qt.DeepEquals, input.RolesRemoved)
	c.Assert(result.RolesRemoved, qt.DeepEquals, input.RolesAdded)
}

func TestReverseSchemaDiff_TableModifications(t *testing.T) {
	c := qt.New(t)

	// Test table modifications reversal
	input := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName:      "users",
				ColumnsAdded:   []string{"email", "created_at"},
				ColumnsRemoved: []string{"legacy_field"},
				ColumnsModified: []types.ColumnDiff{
					{
						ColumnName: "name",
						Changes:    map[string]string{"type": "VARCHAR(100) -> VARCHAR(255)"},
					},
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.TablesModified), qt.Equals, 1)

	reversedTable := result.TablesModified[0]
	c.Assert(reversedTable.TableName, qt.Equals, "users")
	c.Assert(reversedTable.ColumnsAdded, qt.DeepEquals, []string{"legacy_field"})
	c.Assert(reversedTable.ColumnsRemoved, qt.DeepEquals, []string{"email", "created_at"})

	c.Assert(len(reversedTable.ColumnsModified), qt.Equals, 1)
	reversedColumn := reversedTable.ColumnsModified[0]
	c.Assert(reversedColumn.ColumnName, qt.Equals, "name")
	c.Assert(reversedColumn.Changes["type"], qt.Equals, "VARCHAR(255) -> VARCHAR(100)")
}

func TestReverseSchemaDiff_EnumModifications(t *testing.T) {
	c := qt.New(t)

	// Test enum modifications reversal
	input := &types.SchemaDiff{
		EnumsModified: []types.EnumDiff{
			{
				EnumName:      "status_type",
				ValuesAdded:   []string{"pending", "archived"},
				ValuesRemoved: []string{"deprecated"},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.EnumsModified), qt.Equals, 1)

	reversedEnum := result.EnumsModified[0]
	c.Assert(reversedEnum.EnumName, qt.Equals, "status_type")
	c.Assert(reversedEnum.ValuesAdded, qt.DeepEquals, []string{"deprecated"})
	c.Assert(reversedEnum.ValuesRemoved, qt.DeepEquals, []string{"pending", "archived"})
}

func TestReverseSchemaDiff_FunctionModifications(t *testing.T) {
	c := qt.New(t)

	// Test function modifications reversal
	input := &types.SchemaDiff{
		FunctionsModified: []types.FunctionDiff{
			{
				FunctionName: "get_tenant_id",
				Changes: map[string]string{
					"parameters": "() -> (tenant_id TEXT)",
					"body":       "SELECT current_user -> SELECT current_setting('app.tenant_id')",
					"volatility": "VOLATILE -> STABLE",
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.FunctionsModified), qt.Equals, 1)

	reversedFunction := result.FunctionsModified[0]
	c.Assert(reversedFunction.FunctionName, qt.Equals, "get_tenant_id")
	c.Assert(reversedFunction.Changes["parameters"], qt.Equals, "(tenant_id TEXT) -> ()")
	c.Assert(reversedFunction.Changes["body"], qt.Equals, "SELECT current_setting('app.tenant_id') -> SELECT current_user")
	c.Assert(reversedFunction.Changes["volatility"], qt.Equals, "STABLE -> VOLATILE")
}

func TestReverseSchemaDiff_RLSPolicyModifications(t *testing.T) {
	c := qt.New(t)

	// Test RLS policy modifications reversal
	input := &types.SchemaDiff{
		RLSPoliciesModified: []types.RLSPolicyDiff{
			{
				PolicyName: "user_tenant_isolation",
				TableName:  "users",
				Changes: map[string]string{
					"using_expression":      "tenant_id = current_user -> tenant_id = get_current_tenant_id()",
					"with_check_expression": "tenant_id = current_user -> tenant_id = get_current_tenant_id()",
					"to_roles":              "app_user -> app_user,admin_user",
					"policy_for":            "SELECT -> ALL",
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.RLSPoliciesModified), qt.Equals, 1)

	reversedPolicy := result.RLSPoliciesModified[0]
	c.Assert(reversedPolicy.PolicyName, qt.Equals, "user_tenant_isolation")
	c.Assert(reversedPolicy.TableName, qt.Equals, "users")
	c.Assert(reversedPolicy.Changes["using_expression"], qt.Equals, "tenant_id = get_current_tenant_id() -> tenant_id = current_user")
	c.Assert(reversedPolicy.Changes["with_check_expression"], qt.Equals, "tenant_id = get_current_tenant_id() -> tenant_id = current_user")
	c.Assert(reversedPolicy.Changes["to_roles"], qt.Equals, "app_user,admin_user -> app_user")
	c.Assert(reversedPolicy.Changes["policy_for"], qt.Equals, "ALL -> SELECT")
}

func TestReverseSchemaDiff_RoleModifications(t *testing.T) {
	c := qt.New(t)

	// Test role modifications reversal
	input := &types.SchemaDiff{
		RolesModified: []types.RoleDiff{
			{
				RoleName: "app_user",
				Changes: map[string]string{
					"login":     "false -> true",
					"superuser": "false -> true",
					"createdb":  "false -> true",
					"password":  "old_hash -> new_hash",
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.RolesModified), qt.Equals, 1)

	reversedRole := result.RolesModified[0]
	c.Assert(reversedRole.RoleName, qt.Equals, "app_user")
	c.Assert(reversedRole.Changes["login"], qt.Equals, "true -> false")
	c.Assert(reversedRole.Changes["superuser"], qt.Equals, "true -> false")
	c.Assert(reversedRole.Changes["createdb"], qt.Equals, "true -> false")
	c.Assert(reversedRole.Changes["password"], qt.Equals, "new_hash -> old_hash")
}

func TestConvertRLSPolicyRefsToNames(t *testing.T) {
	c := qt.New(t)

	input := []types.RLSPolicyRef{
		{PolicyName: "user_policy", TableName: "users"},
		{PolicyName: "tenant_policy", TableName: "tenants"},
	}

	result := convertRLSPolicyRefsToNames(input)

	expected := []string{"user_policy", "tenant_policy"}
	c.Assert(result, qt.DeepEquals, expected)
}

func TestConvertRLSPolicyNamesToRefs(t *testing.T) {
	c := qt.New(t)

	input := []string{"user_policy", "tenant_policy"}

	result := convertRLSPolicyNamesToRefs(input)

	expected := []types.RLSPolicyRef{
		{PolicyName: "user_policy", TableName: ""},
		{PolicyName: "tenant_policy", TableName: ""},
	}
	c.Assert(result, qt.DeepEquals, expected)
}

func TestReverseSchemaDiff_Issue39_Integration(t *testing.T) {
	c := qt.New(t)

	// This test demonstrates the fix for GitHub issue #39:
	// Migration generator creates incomplete down migrations - missing RLS policies, functions, and roles

	// Create a schema diff that includes RLS policies, functions, and roles being added
	// (simulating a migration that creates these objects)
	upDiff := &types.SchemaDiff{
		// Add some functions
		FunctionsAdded: []string{"get_current_tenant_id", "set_tenant_context"},

		// Add some RLS policies
		RLSPoliciesAdded: []string{"user_tenant_isolation", "area_tenant_isolation"},

		// Enable RLS on tables
		RLSEnabledTablesAdded: []string{"users", "areas"},

		// Add some roles
		RolesAdded: []string{"inventario_app"},

		// Also add a table to make it a realistic scenario
		TablesAdded: []string{"users"},
	}

	// Generate the reverse diff (for down migration)
	downDiff := reverseSchemaDiff(upDiff)

	// Verify that the down migration includes removal of all the objects that were added

	// Functions should be removed in down migration
	c.Assert(downDiff.FunctionsRemoved, qt.DeepEquals, []string{"get_current_tenant_id", "set_tenant_context"})
	c.Assert(len(downDiff.FunctionsAdded), qt.Equals, 0)

	// RLS policies should be removed in down migration
	expectedPolicyRefs := []types.RLSPolicyRef{
		{PolicyName: "user_tenant_isolation", TableName: ""},
		{PolicyName: "area_tenant_isolation", TableName: ""},
	}
	c.Assert(downDiff.RLSPoliciesRemoved, qt.DeepEquals, expectedPolicyRefs)
	c.Assert(len(downDiff.RLSPoliciesAdded), qt.Equals, 0)

	// RLS should be disabled on tables in down migration
	c.Assert(downDiff.RLSEnabledTablesRemoved, qt.DeepEquals, []string{"users", "areas"})
	c.Assert(len(downDiff.RLSEnabledTablesAdded), qt.Equals, 0)

	// Roles should be removed in down migration
	c.Assert(downDiff.RolesRemoved, qt.DeepEquals, []string{"inventario_app"})
	c.Assert(len(downDiff.RolesAdded), qt.Equals, 0)

	// Tables should be removed in down migration (existing behavior)
	c.Assert(downDiff.TablesRemoved, qt.DeepEquals, []string{"users"})
	c.Assert(len(downDiff.TablesAdded), qt.Equals, 0)
}
