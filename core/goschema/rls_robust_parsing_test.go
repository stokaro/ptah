package goschema_test

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
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
			tempFile := filepath.Join(t.TempDir(), "test.go")
			c.Assert(os.WriteFile(tempFile, []byte(tt.goCode), 0600), qt.IsNil)

			// Parse the file
			database := mustParseFile(c, tempFile)

			policies := ownerPolicies(c, &database)
			switches := ownerSwitches(c, &database)
			c.Assert(policies, qt.HasLen, tt.expectedPolicies)
			c.Assert(switches, qt.HasLen, tt.expectedEnabledTables)
			for _, expectedName := range tt.expectedPolicyNames {
				found := slices.ContainsFunc(slices.Collect(maps.Keys(policies)), func(key string) bool { return strings.HasSuffix(key, "."+expectedName) })
				c.Assert(found, qt.IsTrue, qt.Commentf("Expected policy %s not found", expectedName))
			}
			for _, expectedName := range tt.expectedTableNames {
				c.Assert(slices.Collect(maps.Keys(switches)), qt.Contains, expectedName)
			}
		})
	}
}
