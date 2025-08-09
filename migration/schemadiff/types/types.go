package types

// SchemaDiff represents comprehensive differences between two database schemas.
//
// This structure captures all types of schema changes that can occur between a target
// schema (generated from Go struct annotations) and an existing database schema.
// It provides a complete picture of what needs to be modified to bring the database
// schema in line with the application's expected schema.
//
// # Structure Organization
//
// The diff is organized by database object type for clear categorization:
//   - Tables: New, removed, and modified table structures
//   - Enums: New, removed, and modified enum types
//   - Indexes: New and removed database indexes
//
// # JSON Serialization
//
// All fields are JSON-serializable for integration with external tools,
// CI/CD pipelines, and migration management systems.
//
// # Example Usage
//
//	diff := &SchemaDiff{
//		TablesAdded: []string{"users", "posts"},
//		TablesModified: []TableDiff{
//			{TableName: "products", ColumnsAdded: []string{"price", "category"}},
//		},
//		EnumsAdded: []string{"status_type"},
//	}
//
//	if diff.HasChanges() {
//		fmt.Printf("Found %d new tables\n", len(diff.TablesAdded))
//	}
type SchemaDiff struct {
	// TablesAdded contains names of tables that exist in the target schema
	// but not in the current database schema
	TablesAdded []string `json:"tables_added"`

	// TablesRemoved contains names of tables that exist in the current database
	// but not in the target schema (potentially dangerous - data loss)
	TablesRemoved []string `json:"tables_removed"`

	// TablesModified contains detailed information about tables that exist in both
	// schemas but have structural differences (columns, constraints, etc.)
	TablesModified []TableDiff `json:"tables_modified"`

	// EnumsAdded contains names of enum types that exist in the target schema
	// but not in the current database schema
	EnumsAdded []string `json:"enums_added"`

	// EnumsRemoved contains names of enum types that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing data)
	EnumsRemoved []string `json:"enums_removed"`

	// EnumsModified contains detailed information about enum types that exist in both
	// schemas but have different values (additions/removals)
	EnumsModified []EnumDiff `json:"enums_modified"`

	// IndexesAdded contains names of indexes that exist in the target schema
	// but not in the current database schema
	IndexesAdded []string `json:"indexes_added"`

	// IndexesRemoved contains names of indexes that exist in the current database
	// but not in the target schema (safe operation - no data loss)
	IndexesRemoved []string `json:"indexes_removed"`

	// ExtensionsAdded contains names of PostgreSQL extensions that exist in the target schema
	// but not in the current database schema
	ExtensionsAdded []string `json:"extensions_added"`

	// ExtensionsRemoved contains names of PostgreSQL extensions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	ExtensionsRemoved []string `json:"extensions_removed"`

	// FunctionsAdded contains names of PostgreSQL functions that exist in the target schema
	// but not in the current database schema
	FunctionsAdded []string `json:"functions_added"`

	// FunctionsRemoved contains names of PostgreSQL functions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	FunctionsRemoved []string `json:"functions_removed"`

	// FunctionsModified contains detailed information about functions that exist in both
	// schemas but have different definitions (parameters, body, attributes, etc.)
	FunctionsModified []FunctionDiff `json:"functions_modified"`

	// RLSPoliciesAdded contains names of RLS policies that exist in the target schema
	// but not in the current database schema
	RLSPoliciesAdded []string `json:"rls_policies_added"`

	// RLSPoliciesRemoved contains RLS policies that exist in the current database
	// but not in the target schema (safe operation - no data loss)
	RLSPoliciesRemoved []RLSPolicyRef `json:"rls_policies_removed"`

	// RLSPoliciesModified contains detailed information about RLS policies that exist in both
	// schemas but have different definitions (expressions, roles, etc.)
	RLSPoliciesModified []RLSPolicyDiff `json:"rls_policies_modified"`

	// RLSEnabledTablesAdded contains names of tables that need RLS enabled
	RLSEnabledTablesAdded []string `json:"rls_enabled_tables_added"`

	// RLSEnabledTablesRemoved contains names of tables that need RLS disabled
	// (potentially dangerous - removes row-level security)
	RLSEnabledTablesRemoved []string `json:"rls_enabled_tables_removed"`

	// RolesAdded contains names of PostgreSQL roles that exist in the target schema
	// but not in the current database schema
	RolesAdded []string `json:"roles_added"`

	// RolesRemoved contains names of PostgreSQL roles that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	RolesRemoved []string `json:"roles_removed"`

	// RolesModified contains detailed information about roles that exist in both
	// schemas but have different definitions (attributes, passwords, etc.)
	RolesModified []RoleDiff `json:"roles_modified"`
}

// HasChanges returns true if the diff contains any schema changes requiring migration.
//
// This method provides a quick way to determine if any migration actions are needed
// without having to check each individual diff category. It's commonly used in
// CI/CD pipelines and automated deployment systems to decide whether to generate
// and apply migrations.
//
// # Return Value
//
// Returns true if any of the following conditions are met:
//   - New tables need to be created
//   - Existing tables need to be removed
//   - Existing tables have structural modifications
//   - New enum types need to be created
//   - Existing enum types need to be removed
//   - Existing enum types have value modifications
//   - New indexes need to be created
//   - Existing indexes need to be removed
//
// # Example Usage
//
//	diff := CompareSchemas(generated, database)
//	if diff.HasChanges() {
//		log.Println("Schema changes detected, generating migration...")
//		statements := diff.GenerateMigrationAST(generated, "postgres")
//		// Apply migration statements...
//	} else {
//		log.Println("No schema changes detected")
//	}
func (d *SchemaDiff) HasChanges() bool {
	return d.hasTableChanges() ||
		d.hasEnumChanges() ||
		d.hasIndexChanges() ||
		d.hasExtensionChanges() ||
		d.hasFunctionChanges() ||
		d.hasRLSChanges() ||
		d.hasRoleChanges()
}

// hasTableChanges returns true if there are any table-related changes
func (d *SchemaDiff) hasTableChanges() bool {
	return len(d.TablesAdded) > 0 ||
		len(d.TablesRemoved) > 0 ||
		len(d.TablesModified) > 0
}

// hasEnumChanges returns true if there are any enum-related changes
func (d *SchemaDiff) hasEnumChanges() bool {
	return len(d.EnumsAdded) > 0 ||
		len(d.EnumsRemoved) > 0 ||
		len(d.EnumsModified) > 0
}

// hasIndexChanges returns true if there are any index-related changes
func (d *SchemaDiff) hasIndexChanges() bool {
	return len(d.IndexesAdded) > 0 ||
		len(d.IndexesRemoved) > 0
}

// hasExtensionChanges returns true if there are any extension-related changes
func (d *SchemaDiff) hasExtensionChanges() bool {
	return len(d.ExtensionsAdded) > 0 ||
		len(d.ExtensionsRemoved) > 0
}

// hasFunctionChanges returns true if there are any function-related changes
func (d *SchemaDiff) hasFunctionChanges() bool {
	return len(d.FunctionsAdded) > 0 ||
		len(d.FunctionsRemoved) > 0 ||
		len(d.FunctionsModified) > 0
}

// hasRLSChanges returns true if there are any RLS-related changes
func (d *SchemaDiff) hasRLSChanges() bool {
	return len(d.RLSPoliciesAdded) > 0 ||
		len(d.RLSPoliciesRemoved) > 0 ||
		len(d.RLSPoliciesModified) > 0 ||
		len(d.RLSEnabledTablesAdded) > 0 ||
		len(d.RLSEnabledTablesRemoved) > 0
}

// hasRoleChanges returns true if there are any role-related changes
func (d *SchemaDiff) hasRoleChanges() bool {
	return len(d.RolesAdded) > 0 ||
		len(d.RolesRemoved) > 0 ||
		len(d.RolesModified) > 0
}

// hasRoleChanges returns true if there are any role-related changes
func (d *SchemaDiff) hasRoleChanges() bool {
	return len(d.RolesAdded) > 0 ||
		len(d.RolesRemoved) > 0 ||
		len(d.RolesModified) > 0
}

// TableDiff represents structural differences within a specific database table.
//
// This structure captures all types of changes that can occur to a table's structure,
// including column additions, removals, and modifications. It provides detailed
// information needed to generate appropriate ALTER TABLE statements.
//
// # Example Usage
//
//	tableDiff := TableDiff{
//		TableName: "users",
//		ColumnsAdded: []string{"email", "created_at"},
//		ColumnsRemoved: []string{"legacy_field"},
//		ColumnsModified: []ColumnDiff{
//			{ColumnName: "name", Changes: map[string]string{"type": "VARCHAR(100) -> VARCHAR(255)"}},
//		},
//	}
type TableDiff struct {
	// TableName is the name of the table being modified
	TableName string `json:"table_name"`

	// ColumnsAdded contains names of columns that need to be added to the table
	ColumnsAdded []string `json:"columns_added"`

	// ColumnsRemoved contains names of columns that need to be removed from the table
	// (potentially dangerous - may cause data loss)
	ColumnsRemoved []string `json:"columns_removed"`

	// ColumnsModified contains detailed information about columns that exist in both
	// schemas but have different properties (type, constraints, defaults, etc.)
	ColumnsModified []ColumnDiff `json:"columns_modified"`
}

// ColumnDiff represents specific property changes within a database column.
//
// This structure captures the detailed differences between the current column
// definition and the target column definition. Each change is represented as
// a key-value pair showing the transition from old value to new value.
//
// # Change Types
//
// Common change types include:
//   - "type": Data type changes (e.g., "VARCHAR(100) -> VARCHAR(255)")
//   - "nullable": Nullability changes (e.g., "true -> false")
//   - "primary_key": Primary key constraint changes (e.g., "false -> true")
//   - "unique": Unique constraint changes (e.g., "false -> true")
//   - "default": Default value changes (e.g., "'old' -> 'new'")
//
// # Example Usage
//
//	columnDiff := ColumnDiff{
//		ColumnName: "email",
//		Changes: map[string]string{
//			"type": "VARCHAR(100) -> VARCHAR(255)",
//			"nullable": "true -> false",
//			"unique": "false -> true",
//		},
//	}
type ColumnDiff struct {
	// ColumnName is the name of the column being modified
	ColumnName string `json:"column_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// EnumDiff represents changes to enum type values.
//
// This structure captures modifications to enum types, specifically the addition
// and removal of enum values. It's important to note that not all databases
// support enum value removal without recreating the entire enum type.
//
// # Database Limitations
//
//   - **PostgreSQL**: Supports adding enum values but not removing them without recreating the enum
//   - **MySQL/MariaDB**: Supports both adding and removing enum values with ALTER TABLE
//   - **SQLite**: No native enum support - uses CHECK constraints
//
// # Example Usage
//
//	enumDiff := EnumDiff{
//		EnumName: "status_type",
//		ValuesAdded: []string{"pending", "archived"},
//		ValuesRemoved: []string{"deprecated"},
//	}
type EnumDiff struct {
	// EnumName is the name of the enum type being modified
	EnumName string `json:"enum_name"`

	// ValuesAdded contains enum values that need to be added to the enum type
	ValuesAdded []string `json:"values_added"`

	// ValuesRemoved contains enum values that need to be removed from the enum type
	// (may not be supported by all databases - see database limitations above)
	ValuesRemoved []string `json:"values_removed"`
}

// FunctionDiff represents changes to PostgreSQL function definitions.
//
// This structure captures modifications to function definitions, including changes
// to parameters, return types, function body, and function attributes like security
// and volatility. Function modifications typically require dropping and recreating
// the function in PostgreSQL.
//
// # Function Change Types
//
// Common function changes include:
//   - **Parameters**: Changes to function parameter list
//   - **Returns**: Changes to return type
//   - **Body**: Changes to function implementation
//   - **Language**: Changes to function language (rare)
//   - **Security**: Changes between DEFINER and INVOKER
//   - **Volatility**: Changes between STABLE, IMMUTABLE, and VOLATILE
//
// # Example Usage
//
//	functionDiff := FunctionDiff{
//		FunctionName: "get_user_count",
//		Changes: map[string]string{
//			"parameters": "() -> (tenant_id TEXT)",
//			"body": "SELECT COUNT(*) FROM users -> SELECT COUNT(*) FROM users WHERE tenant_id = tenant_id_param",
//			"volatility": "VOLATILE -> STABLE",
//		},
//	}
type FunctionDiff struct {
	// FunctionName is the name of the function being modified
	FunctionName string `json:"function_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// RLSPolicyRef represents a reference to an RLS policy with its table information.
//
// This structure is used to identify RLS policies that need to be dropped,
// providing both the policy name and the table it belongs to. This is necessary
// because PostgreSQL requires both pieces of information for DROP POLICY statements.
//
// # Example Usage
//
//	policyRef := RLSPolicyRef{
//		PolicyName: "user_tenant_isolation",
//		TableName: "users",
//	}
type RLSPolicyRef struct {
	// PolicyName is the name of the RLS policy
	PolicyName string `json:"policy_name"`

	// TableName is the name of the table the policy applies to
	TableName string `json:"table_name"`
}

// RLSPolicyDiff represents changes to Row-Level Security policy definitions.
//
// This structure captures modifications to RLS policies, including changes to
// policy expressions, target roles, and policy types. RLS policy modifications
// typically require dropping and recreating the policy in PostgreSQL.
//
// # Policy Change Types
//
// Common policy changes include:
//   - **PolicyFor**: Changes to policy type (SELECT, INSERT, UPDATE, DELETE, ALL)
//   - **ToRoles**: Changes to target database roles
//   - **UsingExpression**: Changes to USING clause expression
//   - **WithCheckExpression**: Changes to WITH CHECK clause expression
//
// # Example Usage
//
//	policyDiff := RLSPolicyDiff{
//		PolicyName: "user_tenant_isolation",
//		TableName: "users",
//		Changes: map[string]string{
//			"using_expression": "tenant_id = current_user_id() -> tenant_id = get_current_tenant_id()",
//			"to_roles": "app_user -> app_user,admin_user",
//		},
//	}
type RLSPolicyDiff struct {
	// PolicyName is the name of the RLS policy being modified
	PolicyName string `json:"policy_name"`

	// TableName is the name of the table the policy applies to
	TableName string `json:"table_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// RoleDiff represents changes to PostgreSQL role definitions.
//
// This structure captures modifications to role definitions, including changes
// to role attributes such as login capabilities, passwords, privileges, and other
// role properties. Role modifications typically require ALTER ROLE statements in PostgreSQL.
//
// # Role Change Types
//
// Common role changes include:
//   - **Login**: Changes to login capability (true -> false or false -> true)
//   - **Password**: Changes to role password (encrypted)
//   - **Superuser**: Changes to superuser status (true -> false or false -> true)
//   - **CreateDB**: Changes to database creation capability
//   - **CreateRole**: Changes to role creation capability
//   - **Inherit**: Changes to privilege inheritance
//   - **Replication**: Changes to replication capability
//
// # Example Usage
//
//	roleDiff := RoleDiff{
//		RoleName: "app_user",
//		Changes: map[string]string{
//			"login": "false -> true",
//			"password": "old_encrypted_password -> new_encrypted_password",
//			"createdb": "false -> true",
//		},
//	}
type RoleDiff struct {
	// RoleName is the name of the role being modified
	RoleName string `json:"role_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}
