package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
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

//ptah:schema:rls:enable table="nonexistent" comment="Enable RLS for non-existent table"
//ptah:schema:rls:policy name="nonexistent_policy" table="nonexistent" for="ALL" to="app_user" using="true" comment="Policy for non-existent table"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 0,
			description:           "RLS annotations for non-existent tables should be ignored",
		},
		{
			name: "RLS annotation with missing table parameter",
			goCode: `package test

//ptah:schema:rls:enable comment="Enable RLS without table parameter"
//ptah:schema:rls:policy name="invalid_policy" for="ALL" to="app_user" using="true" comment="Policy without table parameter"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 0,
			description:           "RLS annotations without table parameter should be ignored",
		},
		{
			name: "RLS annotation with missing policy name",
			goCode: `package test

//ptah:schema:rls:enable table="users" comment="Enable RLS for users"
//ptah:schema:rls:policy table="users" for="ALL" to="app_user" using="true" comment="Policy without name"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`,
			expectedPolicies:      0,
			expectedEnabledTables: 1,
			description:           "RLS enable should work but policy without name should be ignored",
		},
		{
			name: "RLS annotations in different comment blocks",
			goCode: `package test

// First comment block
//ptah:schema:rls:enable table="users" comment="Enable RLS for users"

// Second comment block
//ptah:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="true" comment="Policy"

// Third comment block
//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
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
			tempFile := filepath.Join(t.TempDir(), "test.go")
			c.Assert(os.WriteFile(tempFile, []byte(tt.goCode), 0600), qt.IsNil)

			// Parse the file
			database := mustParseFile(c, tempFile)

			// Check RLS policies
			c.Assert(ownerPolicies(c, &database), qt.HasLen, tt.expectedPolicies, qt.Commentf("%s", tt.description))

			// Check RLS enabled tables
			c.Assert(ownerSwitches(c, &database), qt.HasLen, tt.expectedEnabledTables, qt.Commentf("%s", tt.description))
		})
	}
}

// TestRLSPolicyEdgeCases_FailurePath pins stokaro/ptah#2440 at the frontend:
// two annotations that declare one policy, or one table's switches, are
// refused, and the refusal names both. Keeping the first would apply a
// definition the author may have meant to replace, and nothing would say the
// second was dropped.
func TestRLSPolicyEdgeCases_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		goCode  string
		wantErr string
	}{
		{
			name: "two policies with one name on one table",
			goCode: `package test

//ptah:schema:rls:enable table="users" comment="Enable RLS for users"
//ptah:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="true" comment="First policy"
//ptah:schema:rls:policy name="user_policy" table="users" for="SELECT" to="app_user" using="true" comment="Duplicate policy"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			wantErr: `(?s).*//ptah:schema:rls:policy at .*test\.go:4 and //ptah:schema:rls:policy at .*test\.go:5 both declare policy "user_policy" on table public\.users; keep one declaration`,
		},
		{
			name: "one table enabled twice",
			goCode: `package test

//ptah:schema:rls:enable table="users" comment="First enable"
//ptah:schema:rls:enable table="users" comment="Duplicate enable"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			wantErr: `(?s).*//ptah:schema:rls:enable at .*test\.go:3 and //ptah:schema:rls:enable at .*test\.go:4 both declare the row-level security switches of table public\.users; keep one declaration`,
		},
		{
			name: "a policy spelled with and without its schema",
			goCode: `package test

//ptah:schema:rls:policy name="tenant" table="orders" for="ALL" using="a = 1"
//ptah:schema:rls:policy name="tenant" table="public.orders" for="ALL" using="b = 2"
//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			wantErr: `(?s).*//ptah:schema:rls:policy at .*test\.go:3 and //ptah:schema:rls:policy at .*test\.go:4 both declare policy "tenant" on table public\.orders; keep one declaration`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempFile := filepath.Join(t.TempDir(), "test.go")
			c.Assert(os.WriteFile(tempFile, []byte(tt.goCode), 0o600), qt.IsNil)

			database, err := goschema.ParseFile(tempFile)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
