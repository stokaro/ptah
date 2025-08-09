package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/goschema/testutil"
)

func TestRLSPolicyEdgeCases(t *testing.T) {
	tests := []struct {
		name                  string
		goCode                string
		expectedPolicies      int
		expectedEnabledTables int
		description           string
	}{
		{
			name: "RLS annotation without corresponding table",
			goCode: `package test

//migrator:schema:rls:enable table="nonexistent" comment="Enable RLS for non-existent table"
//migrator:schema:rls:policy name="nonexistent_policy" table="nonexistent" for="ALL" to="app_user" using="true" comment="Policy for non-existent table"

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 0,
			description:           "RLS annotations for non-existent tables should be ignored",
		},
		{
			name: "RLS annotation with missing table parameter",
			goCode: `package test

//migrator:schema:rls:enable comment="Enable RLS without table parameter"
//migrator:schema:rls:policy name="invalid_policy" for="ALL" to="app_user" using="true" comment="Policy without table parameter"

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 0,
			description:           "RLS annotations without table parameter should be ignored",
		},
		{
			name: "RLS annotation with missing policy name",
			goCode: `package test

//migrator:schema:rls:enable table="users" comment="Enable RLS for users"
//migrator:schema:rls:policy table="users" for="ALL" to="app_user" using="true" comment="Policy without name"

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 1,
			description:           "RLS enable should work but policy without name should be ignored",
		},
		{
			name: "Duplicate RLS policies with same name",
			goCode: `package test

//migrator:schema:rls:enable table="users" comment="Enable RLS for users"
//migrator:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="true" comment="First policy"
//migrator:schema:rls:policy name="user_policy" table="users" for="SELECT" to="app_user" using="true" comment="Duplicate policy"

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			description:           "Duplicate policy names should result in only one policy (first one wins)",
		},
		{
			name: "Duplicate RLS enable for same table",
			goCode: `package test

//migrator:schema:rls:enable table="users" comment="First enable"
//migrator:schema:rls:enable table="users" comment="Duplicate enable"
//migrator:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="true" comment="Policy"

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			description:           "Duplicate RLS enable should result in only one enable (first one wins)",
		},
		{
			name: "RLS annotations in different comment blocks",
			goCode: `package test

// First comment block
//migrator:schema:rls:enable table="users" comment="Enable RLS for users"

// Second comment block
//migrator:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="true" comment="Policy"

// Third comment block
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      1,
			expectedEnabledTables: 1,
			description:           "RLS annotations in separate comment blocks should still be detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create a temporary file with the test code
			tempFile := testutil.CreateTempGoFile(t, tt.goCode)
			defer testutil.RemoveTempFile(t, tempFile)

			// Parse the file
			database := goschema.ParseFile(tempFile)

			// Check RLS policies
			c.Assert(len(database.RLSPolicies), qt.Equals, tt.expectedPolicies,
				qt.Commentf("Expected %d RLS policies, got %d. %s", tt.expectedPolicies, len(database.RLSPolicies), tt.description))

			// Check RLS enabled tables
			c.Assert(len(database.RLSEnabledTables), qt.Equals, tt.expectedEnabledTables,
				qt.Commentf("Expected %d RLS enabled tables, got %d. %s", tt.expectedEnabledTables, len(database.RLSEnabledTables), tt.description))
		})
	}
}
