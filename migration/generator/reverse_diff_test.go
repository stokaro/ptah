package generator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff"
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

func TestGenerateDownMigrationSQL_Issue43_RLSPolicyTableNames(t *testing.T) {
	c := qt.New(t)

	// This test reproduces the exact bug scenario from GitHub issue #43:
	// Missing table names in generated down migration DROP POLICY statements

	// Create a schema diff that adds RLS policies (simulating an up migration)
	upDiff := &types.SchemaDiff{
		RLSPoliciesAdded: []string{"area_user_isolation", "commodity_user_isolation"},
		TablesAdded:      []string{"areas", "commodities"},
	}

	// Create a database schema that includes the RLS policies with table names
	// This simulates the database state after the up migration has been applied
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "areas", RLSEnabled: true},
			{Name: "commodities", RLSEnabled: true},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{
				Name:            "area_user_isolation",
				Table:           "areas",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "user_id = get_current_user_id()",
			},
			{
				Name:            "commodity_user_isolation",
				Table:           "commodities",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "user_id = get_current_user_id()",
			},
		},
	}

	// Create a generated schema that includes the RLS policies with table names
	// This simulates the original generated schema that was used to create the up migration
	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "areas"},
			{Name: "commodities"},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "areas"},
			{Table: "commodities"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{
				Name:            "area_user_isolation",
				Table:           "areas",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "user_id = get_current_user_id()",
			},
			{
				Name:            "commodity_user_isolation",
				Table:           "commodities",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "user_id = get_current_user_id()",
			},
		},
	}

	// Generate down migration SQL
	downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, dbSchema, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)
	downSQL = legacyRenderedSQL(downSQL)

	// Verify that the down migration contains proper DROP POLICY statements with table names
	c.Assert(downSQL, qt.Contains, "DROP POLICY IF EXISTS area_user_isolation ON areas")
	c.Assert(downSQL, qt.Contains, "DROP POLICY IF EXISTS commodity_user_isolation ON commodities")

	// Verify that the malformed statements (without table names) are NOT present
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP POLICY IF EXISTS area_user_isolation ON;")
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP POLICY IF EXISTS commodity_user_isolation ON;")

	// Log the generated SQL for debugging
	t.Logf("Generated down migration SQL:\n%s", downSQL)
}

func TestGenerateDownMigrationSQL_Issue57_MissingTableNames(t *testing.T) {
	c := qt.New(t)

	// This test reproduces the exact bug scenario from GitHub issue #57:
	// Missing table names in generated down migration DROP POLICY statements
	// when the schema context doesn't contain the policy information

	// Create a schema diff that adds RLS policies (simulating an up migration)
	upDiff := &types.SchemaDiff{
		RLSPoliciesAdded: []string{"area_tenant_isolation", "area_user_isolation", "commodity_tenant_isolation"},
		TablesAdded:      []string{"areas", "commodities"},
	}

	// Create a database schema that includes the tables but NOT the RLS policies
	// This simulates the scenario where the schema context is incomplete
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "areas", RLSEnabled: true},
			{Name: "commodities", RLSEnabled: true},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{}, // Empty - this is the key to reproducing the bug
	}

	// Create a generated schema that includes the RLS policies with table names
	// This simulates the original generated schema that was used to create the up migration
	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "areas"},
			{Name: "commodities"},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "areas"},
			{Table: "commodities"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{
				Name:            "area_tenant_isolation",
				Table:           "areas",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "tenant_id = get_current_tenant_id()",
			},
			{
				Name:            "area_user_isolation",
				Table:           "areas",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "user_id = get_current_user_id()",
			},
			{
				Name:            "commodity_tenant_isolation",
				Table:           "commodities",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "tenant_id = get_current_tenant_id()",
			},
		},
	}

	// Generate down migration SQL
	downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, dbSchema, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	// After the fix, these assertions should pass - the table names should be present
	c.Assert(downSQL, qt.Contains, "DROP POLICY IF EXISTS area_tenant_isolation ON areas")
	c.Assert(downSQL, qt.Contains, "DROP POLICY IF EXISTS area_user_isolation ON areas")
	c.Assert(downSQL, qt.Contains, "DROP POLICY IF EXISTS commodity_tenant_isolation ON commodities")

	// Verify that the malformed statements (without table names) are NOT present
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP POLICY IF EXISTS area_tenant_isolation ON;")
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP POLICY IF EXISTS area_user_isolation ON;")
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP POLICY IF EXISTS commodity_tenant_isolation ON;")

	// Log the generated SQL for debugging
	t.Logf("Generated down migration SQL:\n%s", downSQL)
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
		GrantsAdded: []types.GrantRef{
			{Role: "app_user", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantsRemoved: []types.GrantRef{
			{Role: "old_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantOptionsAdded: []types.GrantRef{
			{Role: "app_user", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
		GrantOptionsRevoked: []types.GrantRef{
			{Role: "old_role", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
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
	c.Assert(result.GrantsAdded, qt.DeepEquals, input.GrantsRemoved)
	c.Assert(result.GrantsRemoved, qt.DeepEquals, input.GrantsAdded)
	c.Assert(result.GrantOptionsAdded, qt.DeepEquals, input.GrantOptionsRevoked)
	c.Assert(result.GrantOptionsRevoked, qt.DeepEquals, input.GrantOptionsAdded)
}

func TestGenerateDownMigrationSQL_DropsFKChainChildBeforeParent(t *testing.T) {
	c := qt.New(t)
	schema := fkOrderSchema()
	upDiff := &types.SchemaDiff{
		TablesAdded: []string{
			"ptah_fk_order_accounts",
			"ptah_fk_order_projects",
			"ptah_fk_order_tasks",
		},
	}

	downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, "postgres")
	downSQL = legacyRenderedSQL(downSQL)

	c.Assert(err, qt.IsNil)
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
}

func TestGenerateDownMigrationSQL_DropsFKDiamondLeavesBeforeRoot(t *testing.T) {
	c := qt.New(t)
	schema := fkOrderSchema()
	upDiff := &types.SchemaDiff{
		TablesAdded: []string{
			"ptah_fk_order_accounts",
			"ptah_fk_order_memberships",
			"ptah_fk_order_projects",
			"ptah_fk_order_tasks",
		},
	}

	downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, "postgres")
	downSQL = legacyRenderedSQL(downSQL)

	c.Assert(err, qt.IsNil)
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_memberships")
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_memberships", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
}

func TestGenerateDownMigrationSQL_DropsSchemaQualifiedTableLevelFKChildBeforeParent(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Account", Schema: "app", Name: "accounts"},
			{StructName: "Project", Schema: "app", Name: "projects"},
		},
		Fields: []goschema.Field{
			{StructName: "Account", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "Project", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "Project", Name: "account_id", Type: "VARCHAR(36)"},
		},
		Constraints: []goschema.Constraint{
			{
				Name:           "fk_projects_account",
				Type:           "FOREIGN KEY",
				Table:          "app.projects",
				Columns:        []string{"account_id"},
				ForeignTable:   "app.accounts",
				ForeignColumns: []string{"id"},
			},
		},
	}
	upDiff := &types.SchemaDiff{TablesAdded: []string{"app.accounts", "app.projects"}}

	downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, "postgres")

	c.Assert(err, qt.IsNil)
	assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS app.projects", "DROP TABLE IF EXISTS app.accounts")
}

func TestGenerateDownMigrationSQL_DropsMySQLFamilyFKChainInDependencyOrder(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := fkOrderSchema()
			goschema.Finalize(schema)
			upDiff := schemadiff.CompareWithDialect(schema, &dbschematypes.DBSchema{}, dialect)

			downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, dialect)
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			assertSQLBefore(t, downSQL, "ALTER TABLE ptah_fk_order_tasks DROP FOREIGN KEY", "DROP TABLE IF EXISTS ptah_fk_order_tasks")
			assertSQLBefore(t, downSQL, "ALTER TABLE ptah_fk_order_projects DROP FOREIGN KEY", "DROP TABLE IF EXISTS ptah_fk_order_projects")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
		})
	}
}

func TestGenerateDownMigrationSQL_DropsMySQLFamilyFKDiamondInDependencyOrder(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := fkOrderSchema()
			goschema.Finalize(schema)
			upDiff := schemadiff.CompareWithDialect(schema, &dbschematypes.DBSchema{}, dialect)

			downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, dialect)
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			assertSQLBefore(t, downSQL, "ALTER TABLE ptah_fk_order_tasks DROP FOREIGN KEY", "DROP TABLE IF EXISTS ptah_fk_order_tasks")
			assertSQLBefore(t, downSQL, "ALTER TABLE ptah_fk_order_projects DROP FOREIGN KEY", "DROP TABLE IF EXISTS ptah_fk_order_projects")
			assertSQLBefore(t, downSQL, "ALTER TABLE ptah_fk_order_memberships DROP FOREIGN KEY", "DROP TABLE IF EXISTS ptah_fk_order_memberships")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_memberships")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
			assertSQLBefore(t, downSQL, "DROP TABLE IF EXISTS ptah_fk_order_memberships", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
		})
	}
}

func TestGenerateDownMigrationSQL_DropsMySQLFamilyMutualFKCycleTogether(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := mutualFKCycleSchema()
			goschema.Finalize(schema)
			upDiff := schemadiff.CompareWithDialect(schema, &dbschematypes.DBSchema{}, dialect)

			downSQL, err := generateDownMigrationSQL(upDiff, schema, &dbschematypes.DBSchema{}, dialect)
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			assertSQLBefore(t, downSQL, "ALTER TABLE left_nodes DROP FOREIGN KEY", "DROP TABLE IF EXISTS left_nodes")
			assertSQLBefore(t, downSQL, "ALTER TABLE right_nodes DROP FOREIGN KEY", "DROP TABLE IF EXISTS right_nodes")
			c.Assert(downSQL, qt.Contains, "fk_left_nodes_right_id")
			c.Assert(downSQL, qt.Contains, "fk_right_nodes_left_id")
		})
	}
}

func TestReverseSchemaDiff_GrantOptionUpgradeDownRevokesOnlyOption(t *testing.T) {
	c := qt.New(t)
	upDiff := &types.SchemaDiff{
		GrantOptionsAdded: []types.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		},
	}

	downDiff := reverseSchemaDiff(upDiff)
	downSQL, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(downDiff, &goschema.Database{})...)

	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)
	c.Assert(downSQL, qt.Contains, "REVOKE GRANT OPTION FOR SELECT ON TABLE users FROM app_role;")
	c.Assert(strings.Contains(downSQL, "REVOKE SELECT ON TABLE users FROM app_role;"), qt.Equals, false)
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

	c.Assert(result.TablesModified, qt.HasLen, 1)

	reversedTable := result.TablesModified[0]
	c.Assert(reversedTable.TableName, qt.Equals, "users")
	c.Assert(reversedTable.ColumnsAdded, qt.DeepEquals, []string{"legacy_field"})
	c.Assert(reversedTable.ColumnsRemoved, qt.DeepEquals, []string{"email", "created_at"})

	c.Assert(reversedTable.ColumnsModified, qt.HasLen, 1)
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

	c.Assert(result.EnumsModified, qt.HasLen, 1)

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

	c.Assert(result.FunctionsModified, qt.HasLen, 1)

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

	c.Assert(result.RLSPoliciesModified, qt.HasLen, 1)

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
				Changes: map[string]string{ //nolint:gosec // G101: "password" is a map key naming a field, not a credential
					"login":     "false -> true",
					"superuser": "false -> true",
					"createdb":  "false -> true",
					"password":  "old_hash -> new_hash",
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(result.RolesModified, qt.HasLen, 1)

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

func TestConvertRLSPolicyNamesToRefsWithSchema(t *testing.T) {
	c := qt.New(t)

	input := []string{"user_policy", "tenant_policy", "unknown_policy"}

	// Create a mock schema with RLS policies
	schema := &goschema.Database{
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "user_policy", Table: "users"},
			{Name: "tenant_policy", Table: "tenants"},
			// Note: unknown_policy is not in the schema
		},
	}

	result := convertRLSPolicyNamesToRefsWithSchema(input, schema)

	expected := []types.RLSPolicyRef{
		{PolicyName: "user_policy", TableName: "users"},
		{PolicyName: "tenant_policy", TableName: "tenants"},
		{PolicyName: "unknown_policy", TableName: ""}, // Table name not found, remains empty
	}
	c.Assert(result, qt.DeepEquals, expected)
}

func TestConvertRLSPolicyNamesToRefsWithSchema_NilSchema(t *testing.T) {
	c := qt.New(t)

	input := []string{"user_policy", "tenant_policy"}

	result := convertRLSPolicyNamesToRefsWithSchema(input, nil)

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
	c.Assert(downDiff.FunctionsAdded, qt.HasLen, 0)

	// RLS policies should be removed in down migration
	expectedPolicyRefs := []types.RLSPolicyRef{
		{PolicyName: "user_tenant_isolation", TableName: ""},
		{PolicyName: "area_tenant_isolation", TableName: ""},
	}
	c.Assert(downDiff.RLSPoliciesRemoved, qt.DeepEquals, expectedPolicyRefs)
	c.Assert(downDiff.RLSPoliciesAdded, qt.HasLen, 0)

	// RLS should be disabled on tables in down migration
	c.Assert(downDiff.RLSEnabledTablesRemoved, qt.DeepEquals, []string{"users", "areas"})
	c.Assert(downDiff.RLSEnabledTablesAdded, qt.HasLen, 0)

	// Roles should be removed in down migration
	c.Assert(downDiff.RolesRemoved, qt.DeepEquals, []string{"inventario_app"})
	c.Assert(downDiff.RolesAdded, qt.HasLen, 0)

	// Tables should be removed in down migration (existing behavior)
	c.Assert(downDiff.TablesRemoved, qt.DeepEquals, []string{"users"})
	c.Assert(downDiff.TablesAdded, qt.HasLen, 0)
}

// TestReverseSchemaDiff_ConstraintReversal verifies that a modified constraint
// (expressed by the comparator as remove + add of the same name) is reversed so
// the down migration drops the new definition and re-adds the old one. This is
// the field-level FK on_delete/on_update drift case from issue #189: without the
// reversal the down migration was empty and could not restore the prior action.
func TestReverseSchemaDiff_ConstraintReversal(t *testing.T) {
	c := qt.New(t)

	input := &types.SchemaDiff{
		// Up: change fk_export_file's action -> remove(old) + add(new) of the
		// same name.
		ConstraintsRemoved: []string{"fk_export_file"},
		ConstraintsAdded:   []string{"fk_export_file"},
	}

	result := reverseSchemaDiff(input)

	// Down: the slices swap, so the down re-adds the old and drops the new.
	c.Assert(result.ConstraintsAdded, qt.DeepEquals, []string{"fk_export_file"})
	c.Assert(result.ConstraintsRemoved, qt.DeepEquals, []string{"fk_export_file"})
}

// TestReverseSchemaDiff_FieldLevelCheckRemovalsWithTables verifies that field-level
// CHECK constraints added by an up migration are reversed into table-qualified
// removals for the down migration. MySQL/MariaDB need the owning table to emit a
// real DROP CONSTRAINT; the bare ConstraintsRemoved name list is not enough.
func TestReverseSchemaDiff_FieldLevelCheckRemovalsWithTables(t *testing.T) {
	c := qt.New(t)

	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "File", Name: "files"}},
		Fields: []goschema.Field{
			{
				StructName: "File",
				Name:       "category",
				Type:       "TEXT",
				Check:      "category IN ('a','b')",
			},
			{
				StructName: "File",
				Name:       "status",
				Type:       "TEXT",
				Check:      "status IN ('new','done')",
				CheckName:  "files_status_valid",
			},
		},
	}
	upDiff := &types.SchemaDiff{
		ConstraintsAdded: []string{"files_category_check", "files_status_valid"},
	}

	result := reverseSchemaDiffWithSchema(upDiff, generatedSchema, nil)

	c.Assert(result.ConstraintsRemoved, qt.DeepEquals, []string{"files_category_check", "files_status_valid"})
	c.Assert(result.ConstraintsRemovedWithTables, qt.DeepEquals, []types.ConstraintRemovalInfo{
		{Name: "files_category_check", TableName: "files", Type: "CHECK"},
		{Name: "files_status_valid", TableName: "files", Type: "CHECK"},
	})
}

// TestReverseSchemaDiff_AddedTableForeignKeyRemovalsWithTables verifies the
// down metadata required by MySQL/MariaDB when a whole table is created by the
// up migration. Those dialects cannot rely on DROP TABLE CASCADE, so FKs owned
// by the added table must be explicitly removed before the table drop even when
// the comparator does not report them as standalone constraint additions.
func TestReverseSchemaDiff_AddedTableForeignKeyRemovalsWithTables(t *testing.T) {
	c := qt.New(t)

	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Account", Schema: "app", Name: "accounts"},
			{StructName: "Project", Schema: "app", Name: "projects"},
			{StructName: "Audit", Schema: "app", Name: "audits"},
		},
		Fields: []goschema.Field{
			{StructName: "Project", Name: "account_id", Type: "INTEGER", Foreign: "app.accounts(id)"},
			{
				StructName:     "Project",
				Name:           "owner_id",
				Type:           "INTEGER",
				Foreign:        "app.accounts(id)",
				ForeignKeyName: "fk_project_owner_id",
			},
			{StructName: "Audit", Name: "project_id", Type: "INTEGER", Foreign: "app.projects(id)"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "Project", Mode: "relation", Field: "owner_id", Ref: "app.accounts(id)"},
		},
		Constraints: []goschema.Constraint{
			{
				StructName:     "Project",
				Type:           "FOREIGN KEY",
				Table:          "app.projects",
				Columns:        []string{"tenant_id", "reviewer_id"},
				ForeignTable:   "app.accounts",
				ForeignColumns: []string{"tenant_id", "id"},
			},
		},
	}
	upDiff := &types.SchemaDiff{
		TablesAdded: []string{"app.projects"},
	}

	result := reverseSchemaDiffWithSchema(upDiff, generatedSchema, nil)

	c.Assert(result.ConstraintsRemovedWithTables, qt.DeepEquals, []types.ConstraintRemovalInfo{
		{Name: "fk_projects_account_id", TableName: "app.projects", Type: "FOREIGN KEY"},
		{Name: "fk_project_owner_id", TableName: "app.projects", Type: "FOREIGN KEY"},
		{Name: "fk_projects_tenant_id_reviewer_id", TableName: "app.projects", Type: "FOREIGN KEY"},
	})
}

func TestForeignKeyAdditionFromDBConstraint_DeduplicatesRepeatedIntrospectionColumns(t *testing.T) {
	c := qt.New(t)
	foreignTable := "ptah_tenants"
	dbConstraint := dbschematypes.DBConstraint{
		Name:           "fk_entity_tenant",
		TableName:      "ptah_area",
		Type:           "FOREIGN KEY",
		ColumnNames:    []string{"tenant_id", "tenant_id", "tenant_id"},
		ForeignTable:   &foreignTable,
		ForeignColumns: []string{"id", "id", "id"},
	}

	info := foreignKeyAdditionFromDBConstraint("fk_entity_tenant", "ptah_area", dbConstraint)

	c.Assert(info.Columns, qt.DeepEquals, []string{"tenant_id"})
	c.Assert(info.ForeignColumns, qt.DeepEquals, []string{"id"})
	c.Assert(info.ForeignColumn, qt.Equals, "id")
}

func fkOrderSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKOrderAccount", Name: "ptah_fk_order_accounts"},
			{StructName: "PtahFKOrderProject", Name: "ptah_fk_order_projects"},
			{StructName: "PtahFKOrderMembership", Name: "ptah_fk_order_memberships"},
			{StructName: "PtahFKOrderTask", Name: "ptah_fk_order_tasks"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKOrderAccount", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderProject", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderProject",
				Name:           "account_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_accounts(id)",
				ForeignKeyName: "fk_ptah_fk_order_projects_account",
			},
			{StructName: "PtahFKOrderMembership", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderMembership",
				Name:           "account_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_accounts(id)",
				ForeignKeyName: "fk_ptah_fk_order_memberships_account",
			},
			{StructName: "PtahFKOrderTask", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderTask",
				Name:           "project_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_projects(id)",
				ForeignKeyName: "fk_ptah_fk_order_tasks_project",
			},
			{
				StructName:     "PtahFKOrderTask",
				Name:           "membership_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_memberships(id)",
				ForeignKeyName: "fk_ptah_fk_order_tasks_membership",
			},
		},
	}
}

func mutualFKCycleSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "LeftNode", Name: "left_nodes"},
			{StructName: "RightNode", Name: "right_nodes"},
		},
		Fields: []goschema.Field{
			{StructName: "LeftNode", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "LeftNode",
				Name:           "right_id",
				Type:           "INTEGER",
				Foreign:        "right_nodes(id)",
				ForeignKeyName: "fk_left_nodes_right_id",
			},
			{StructName: "RightNode", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "RightNode",
				Name:           "left_id",
				Type:           "INTEGER",
				Foreign:        "left_nodes(id)",
				ForeignKeyName: "fk_right_nodes_left_id",
			},
		},
	}
}

func assertSQLBefore(t *testing.T, sql, earlier, later string) {
	t.Helper()
	c := qt.New(t)
	sql = legacyRenderedSQL(sql)
	earlierIndex := strings.Index(sql, earlier)
	laterIndex := strings.Index(sql, later)
	c.Assert(earlierIndex >= 0, qt.IsTrue, qt.Commentf("%q not found in SQL:\n%s", earlier, sql))
	c.Assert(laterIndex >= 0, qt.IsTrue, qt.Commentf("%q not found in SQL:\n%s", later, sql))
	c.Assert(earlierIndex < laterIndex, qt.IsTrue, qt.Commentf("%q must appear before %q:\n%s", earlier, later, sql))
}

// TestGenerateDownMigrationSQL_Issue189_RestoresPriorForeignKeyAction is the
// acceptance test for the down half of issue #189: when an up migration changes
// a field-level FK's ON DELETE action, the generated down migration must DROP
// the new constraint and re-ADD it with the PRIOR action read back from the
// (pre-change) database state. Previously the down migration was empty.
func TestGenerateDownMigrationSQL_Issue189_RestoresPriorForeignKeyAction(t *testing.T) {
	noAction := "NO ACTION"
	filesTable := "files"
	idCol := "id"

	// Generated (target) schema: file_id FK now uses ON DELETE SET NULL.
	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
		Fields: []goschema.Field{
			{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
			{
				StructName:     "Export",
				Name:           "file_id",
				Type:           "TEXT",
				Nullable:       true,
				Foreign:        "files(id)",
				ForeignKeyName: "fk_export_file",
				OnDelete:       "SET NULL",
			},
		},
	}

	// Database (current, pre-change) schema: the FK still has the prior default
	// NO ACTION. This is what the down migration must restore.
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "exports",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
					{Name: "file_id", DataType: "text", IsNullable: "YES"},
				},
			},
		},
		Constraints: []dbschematypes.DBConstraint{
			{
				Name:          "fk_export_file",
				TableName:     "exports",
				Type:          "FOREIGN KEY",
				ColumnName:    "file_id",
				ForeignTable:  &filesTable,
				ForeignColumn: &idCol,
				DeleteRule:    &noAction,
				UpdateRule:    &noAction,
			},
		},
	}

	// Up diff for the action change: remove(old) + add(new) of the same name.
	upDiff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"fk_export_file"},
		ConstraintsAdded:   []string{"fk_export_file"},
	}

	t.Run("postgres", func(t *testing.T) {
		c := qt.New(t)
		downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, dbSchema, "postgres")
		c.Assert(err, qt.IsNil)
		downSQL = legacyRenderedSQL(downSQL)

		// The down must be non-empty and re-add the FK (restoring the prior
		// action) plus drop the new definition first.
		c.Assert(downSQL, qt.Contains, "ADD CONSTRAINT fk_export_file FOREIGN KEY (file_id) REFERENCES files(id)")
		c.Assert(downSQL, qt.Contains, "DROP CONSTRAINT IF EXISTS")
		// The restored action must NOT be SET NULL (that was the new action).
		c.Assert(downSQL, qt.Not(qt.Contains), "ON DELETE SET NULL",
			qt.Commentf("down must restore the prior action, not SET NULL:\n%s", downSQL))
	})

	t.Run("mysql", func(t *testing.T) {
		c := qt.New(t)
		downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, dbSchema, "mysql")
		c.Assert(err, qt.IsNil)
		downSQL = legacyRenderedSQL(downSQL)

		// Real DROP FOREIGN KEY + re-ADD with the prior action; never a TODO.
		c.Assert(downSQL, qt.Contains, "ALTER TABLE exports DROP FOREIGN KEY fk_export_file;")
		c.Assert(downSQL, qt.Contains, "ADD CONSTRAINT fk_export_file FOREIGN KEY (file_id) REFERENCES files(id)")
		c.Assert(downSQL, qt.Not(qt.Contains), "TODO",
			qt.Commentf("down must not emit a TODO placeholder:\n%s", downSQL))
		c.Assert(downSQL, qt.Not(qt.Contains), "ON DELETE SET NULL",
			qt.Commentf("down must restore the prior action, not SET NULL:\n%s", downSQL))
	})
}

// TestGenerateDownMigrationSQL_Issue194_DropsFieldLevelCheckMySQLFamily is the
// acceptance test for issue #194: adding a field-level CHECK on an existing
// column must generate a non-empty MySQL/MariaDB DOWN that drops the CHECK.
func TestGenerateDownMigrationSQL_Issue194_DropsFieldLevelCheckMySQLFamily(t *testing.T) {
	generatedSchema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "File", Name: "files"}},
		Fields: []goschema.Field{
			{
				StructName: "File",
				Name:       "category",
				Type:       "TEXT",
				Check:      "category IN ('a','b')",
			},
		},
	}
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "files",
				Columns: []dbschematypes.DBColumn{
					{Name: "category", DataType: "text", IsNullable: "YES"},
				},
			},
		},
	}
	upDiff := schemadiff.Compare(generatedSchema, dbSchema)
	c := qt.New(t)
	c.Assert(upDiff.ConstraintsAdded, qt.DeepEquals, []string{"files_category_check"})

	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, dbSchema, dialect)
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			wantDrop := "ALTER TABLE files DROP CONSTRAINT files_category_check;"
			if dialect == "mariadb" {
				wantDrop = "ALTER TABLE files DROP CONSTRAINT IF EXISTS files_category_check;"
			}
			c.Assert(downSQL, qt.Contains, wantDrop)
			c.Assert(downSQL, qt.Not(qt.Contains), "No rollback operations needed",
				qt.Commentf("field-level CHECK down migration must not be empty:\n%s", downSQL))
			c.Assert(downSQL, qt.Not(qt.Contains), "TODO",
				qt.Commentf("down must emit real SQL, not a TODO placeholder:\n%s", downSQL))
		})
	}
}
