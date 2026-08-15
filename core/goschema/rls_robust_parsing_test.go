package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema/testutil"
)

func TestRLSPolicyParsingRobustness(t *testing.T) {
	tests := []struct {
		name                  string
		goCode                string
		expectedPolicies      int
		expectedEnabledTables int
		expectedPolicyNames   []string
		expectedTableNames    []string
	}{
		{
			name: "RLS annotations with blank lines (original issue)",
			goCode: `package test

// Enable RLS for multi-tenant isolation
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_tenant_isolation"},
			expectedTableNames:    []string{"users"},
		},
		{
			name: "RLS annotations with empty comment lines (working case)",
			goCode: `package test

// Enable RLS for multi-tenant isolation
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//
//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_tenant_isolation"},
			expectedTableNames:    []string{"users"},
		},
		{
			name: "RLS annotations directly adjacent to table annotation",
			goCode: `package test

//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_tenant_isolation"},
			expectedTableNames:    []string{"users"},
		},
		{
			name: "Multiple RLS policies for same table",
			goCode: `package test

//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
//ptah:schema:rls:policy name="user_select_policy" table="users" for="SELECT" to="app_user" using="tenant_id = get_current_tenant_id()" comment="Select policy"
//ptah:schema:rls:policy name="user_insert_policy" table="users" for="INSERT" to="app_user" with_check="tenant_id = get_current_tenant_id()" comment="Insert policy"
//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      2,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_select_policy", "user_insert_policy"},
			expectedTableNames:    []string{"users"},
		},
		{
			name: "RLS annotations separated by multiple blank lines",
			goCode: `package test

// Enable RLS for multi-tenant isolation


//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"


//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"



//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_tenant_isolation"},
			expectedTableNames:    []string{"users"},
		},
		{
			name: "RLS annotations with mixed comment styles",
			goCode: `package test

/* Block comment */
// Enable RLS for multi-tenant isolation
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
/* Another block comment */
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			expectedPolicyNames:   []string{"user_tenant_isolation"},
			expectedTableNames:    []string{"users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create a temporary file with the test code
			tempFile := testutil.CreateTempGoFile(t, tt.goCode)
			defer testutil.RemoveTempFile(t, tempFile)

			// Parse the file
			database := mustParseFile(c.TB, tempFile)

			// Check RLS policies
			c.Assert(database.RLSPolicies, qt.HasLen, tt.expectedPolicies,
				qt.Commentf("Expected %d RLS policies, got %d", tt.expectedPolicies, len(database.RLSPolicies)))

			// Check RLS enabled tables
			c.Assert(database.RLSEnabledTables, qt.HasLen, tt.expectedEnabledTables,
				qt.Commentf("Expected %d RLS enabled tables, got %d", tt.expectedEnabledTables, len(database.RLSEnabledTables)))

			// Check policy names
			if len(tt.expectedPolicyNames) > 0 {
				actualPolicyNames := make([]string, len(database.RLSPolicies))
				for i, policy := range database.RLSPolicies {
					actualPolicyNames[i] = policy.Name
				}
				for _, expectedName := range tt.expectedPolicyNames {
					c.Assert(actualPolicyNames, qt.Contains, expectedName,
						qt.Commentf("Expected policy %s not found", expectedName))
				}
			}

			// Check table names
			if len(tt.expectedTableNames) > 0 {
				actualTableNames := make([]string, len(database.RLSEnabledTables))
				for i, table := range database.RLSEnabledTables {
					actualTableNames[i] = table.Table
				}
				for _, expectedName := range tt.expectedTableNames {
					c.Assert(actualTableNames, qt.Contains, expectedName,
						qt.Commentf("Expected RLS enabled table %s not found", expectedName))
				}
			}
		})
	}
}
