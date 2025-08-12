package goschema

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

// Test case to reproduce the bug described in issue #49
// where embedded EntityID structs with primary keys are not properly processed

// EntityID represents a common ID structure with primary key
type EntityID struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string `json:"id" db:"id" userinput:"false"`
}

// TenantAwareEntityID embeds EntityID and adds tenant/user references
type TenantAwareEntityID struct {
	//migrator:embedded mode="inline"
	EntityID
	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true" foreign="tenants(id)" foreign_key_name="fk_entity_tenant"
	TenantID string `json:"tenant_id" db:"tenant_id" userinput:"false"`
	//migrator:schema:field name="user_id" type="TEXT" not_null="true" foreign="users(id)" foreign_key_name="fk_entity_user"
	UserID string `json:"user_id" db:"user_id" userinput:"false"`
}

// User table that embeds TenantAwareEntityID
//
//migrator:schema:table name="users"
type User struct {
	//migrator:embedded mode="inline"
	TenantAwareEntityID
	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string `json:"email" db:"email"`
	//migrator:schema:field name="password_hash" type="TEXT" not_null="true"
	PasswordHash string `json:"password_hash" db:"password_hash"`
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`
	//migrator:schema:field name="role" type="TEXT" not_null="true" default="user"
	Role string `json:"role" db:"role"`
	//migrator:schema:field name="is_active" type="BOOLEAN" not_null="true" default="true"
	IsActive bool `json:"is_active" db:"is_active"`
	//migrator:schema:field name="last_login_at" type="TIMESTAMP"
	LastLoginAt *string `json:"last_login_at" db:"last_login_at"`
	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true"
	CreatedAt string `json:"created_at" db:"created_at"`
	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true"
	UpdatedAt string `json:"updated_at" db:"updated_at"`
}

// Tenant table for reference
//
//migrator:schema:table name="tenants"
type Tenant struct {
	//migrator:embedded mode="inline"
	EntityID
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`
}

// Area table that references users
//
//migrator:schema:table name="areas"
type Area struct {
	//migrator:embedded mode="inline"
	TenantAwareEntityID
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`
}

func TestEmbeddedEntityIDBugReproduction(t *testing.T) {
	c := qt.New(t)

	// Parse the test file to get the schema
	database := ParseFile("embedded_bug_reproduction_test.go")

	// Process embedded fields to get all fields
	allFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	// Check that we have the expected tables (users, tenants, areas, prefixed_test)
	c.Assert(len(database.Tables), qt.Equals, 4)

	// Find the users table
	var usersTable Table
	for _, table := range database.Tables {
		if table.Name == "users" {
			usersTable = table
			break
		}
	}
	c.Assert(usersTable.Name, qt.Equals, "users")

	// Get all fields for the users table
	var usersFields []Field
	for _, field := range allFields {
		if field.StructName == usersTable.StructName {
			usersFields = append(usersFields, field)
		}
	}

	// Check that the users table has an id field (this should fail with the current bug)
	var hasIDField bool
	var idField Field
	for _, field := range usersFields {
		if field.Name == "id" {
			hasIDField = true
			idField = field
			break
		}
	}

	// This assertion should pass but currently fails due to the bug
	c.Assert(hasIDField, qt.IsTrue, qt.Commentf("Users table should have an 'id' field from embedded EntityID"))
	c.Assert(idField.Primary, qt.IsTrue, qt.Commentf("The id field should be marked as primary key"))
	c.Assert(idField.Type, qt.Equals, "TEXT")

	// Check that we have all expected fields for users table
	expectedFields := []string{"id", "tenant_id", "user_id", "email", "password_hash", "name", "role", "is_active", "last_login_at", "created_at", "updated_at"}
	actualFieldNames := make([]string, len(usersFields))
	for i, field := range usersFields {
		actualFieldNames[i] = field.Name
	}

	for _, expectedField := range expectedFields {
		found := false
		for _, actualField := range actualFieldNames {
			if actualField == expectedField {
				found = true
				break
			}
		}
		c.Assert(found, qt.IsTrue, qt.Commentf("Expected field '%s' not found in users table. Actual fields: %v", expectedField, actualFieldNames))
	}

	// Check that tenants table also has id field
	var tenantsTable Table
	for _, table := range database.Tables {
		if table.Name == "tenants" {
			tenantsTable = table
			break
		}
	}
	c.Assert(tenantsTable.Name, qt.Equals, "tenants")

	var tenantsFields []Field
	for _, field := range allFields {
		if field.StructName == tenantsTable.StructName {
			tenantsFields = append(tenantsFields, field)
		}
	}

	var tenantsHasIDField bool
	for _, field := range tenantsFields {
		if field.Name == "id" {
			tenantsHasIDField = true
			break
		}
	}
	c.Assert(tenantsHasIDField, qt.IsTrue, qt.Commentf("Tenants table should have an 'id' field from embedded EntityID"))
}

func TestEmbeddedFieldProcessingDebug(t *testing.T) {
	c := qt.New(t)

	// Parse the test file to get the schema
	database := ParseFile("embedded_bug_reproduction_test.go")

	t.Logf("Found %d tables", len(database.Tables))
	for _, table := range database.Tables {
		t.Logf("Table: %s (struct: %s)", table.Name, table.StructName)
	}

	t.Logf("Found %d original fields", len(database.Fields))
	for _, field := range database.Fields {
		t.Logf("Original field: %s.%s (type: %s, primary: %t)", field.StructName, field.Name, field.Type, field.Primary)
	}

	t.Logf("Found %d embedded fields", len(database.EmbeddedFields))
	for _, embedded := range database.EmbeddedFields {
		t.Logf("Embedded field: %s.%s (mode: %s, type: %s)", embedded.StructName, embedded.EmbeddedTypeName, embedded.Mode, embedded.EmbeddedTypeName)
	}

	// Process embedded fields
	allFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	t.Logf("After processing embedded fields, found %d total fields", len(allFields))
	for _, field := range allFields {
		t.Logf("All field: %s.%s (type: %s, primary: %t)", field.StructName, field.Name, field.Type, field.Primary)
	}

	// Check specifically for User struct fields
	var userFields []Field
	for _, field := range allFields {
		if field.StructName == "User" {
			userFields = append(userFields, field)
		}
	}

	t.Logf("User struct has %d fields:", len(userFields))
	for _, field := range userFields {
		t.Logf("  - %s (type: %s, primary: %t)", field.Name, field.Type, field.Primary)
	}

	// This test is just for debugging - we expect it to show the missing id field
	c.Assert(len(userFields), qt.Not(qt.Equals), 0)
}

func TestNestedEmbeddedFieldsComprehensive(t *testing.T) {
	c := qt.New(t)

	// Parse the test file to get the schema
	database := ParseFile("embedded_bug_reproduction_test.go")

	// Process embedded fields to get all fields
	allFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	// Test 1: Verify User table has all expected fields including nested ones
	var userFields []Field
	for _, field := range allFields {
		if field.StructName == "User" {
			userFields = append(userFields, field)
		}
	}

	expectedUserFields := map[string]bool{
		"id":            true, // From EntityID (nested)
		"tenant_id":     true, // From TenantAwareEntityID
		"user_id":       true, // From TenantAwareEntityID
		"email":         true, // Direct field
		"password_hash": true, // Direct field
		"name":          true, // Direct field
		"role":          true, // Direct field
		"is_active":     true, // Direct field
		"last_login_at": true, // Direct field
		"created_at":    true, // Direct field
		"updated_at":    true, // Direct field
	}

	actualUserFields := make(map[string]Field)
	for _, field := range userFields {
		actualUserFields[field.Name] = field
	}

	// Check that all expected fields are present
	for expectedField := range expectedUserFields {
		field, exists := actualUserFields[expectedField]
		c.Assert(exists, qt.IsTrue, qt.Commentf("Expected field '%s' not found in User table", expectedField))

		// Special check for the id field - it should be primary
		if expectedField == "id" {
			c.Assert(field.Primary, qt.IsTrue, qt.Commentf("The id field should be marked as primary key"))
			c.Assert(field.Type, qt.Equals, "TEXT")
		}
	}

	// Test 2: Verify Tenant table has id field from EntityID
	var tenantFields []Field
	for _, field := range allFields {
		if field.StructName == "Tenant" {
			tenantFields = append(tenantFields, field)
		}
	}

	var tenantHasID bool
	for _, field := range tenantFields {
		if field.Name == "id" && field.Primary {
			tenantHasID = true
			break
		}
	}
	c.Assert(tenantHasID, qt.IsTrue, qt.Commentf("Tenant table should have primary key id field"))

	// Test 3: Verify Area table has all fields from nested embedding
	var areaFields []Field
	for _, field := range allFields {
		if field.StructName == "Area" {
			areaFields = append(areaFields, field)
		}
	}

	expectedAreaFields := map[string]bool{
		"id":        true, // From EntityID (nested through TenantAwareEntityID)
		"tenant_id": true, // From TenantAwareEntityID
		"user_id":   true, // From TenantAwareEntityID
		"name":      true, // Direct field
	}

	actualAreaFields := make(map[string]Field)
	for _, field := range areaFields {
		actualAreaFields[field.Name] = field
	}

	for expectedField := range expectedAreaFields {
		field, exists := actualAreaFields[expectedField]
		c.Assert(exists, qt.IsTrue, qt.Commentf("Expected field '%s' not found in Area table", expectedField))

		// Special check for the id field - it should be primary
		if expectedField == "id" {
			c.Assert(field.Primary, qt.IsTrue, qt.Commentf("The id field should be marked as primary key"))
		}
	}

	// Test 4: Verify no duplicate fields are generated
	fieldCounts := make(map[string]int)
	for _, field := range userFields {
		fieldCounts[field.Name]++
	}

	for fieldName, count := range fieldCounts {
		c.Assert(count, qt.Equals, 1, qt.Commentf("Field '%s' appears %d times in User table, should appear exactly once", fieldName, count))
	}
}

// Test nested embedded fields with prefixes
type PrefixedEntityID struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string `json:"id" db:"id"`
}

type PrefixedTenantAware struct {
	//migrator:embedded mode="inline" prefix="entity_"
	PrefixedEntityID
	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`
}

//migrator:schema:table name="prefixed_test"
type PrefixedTest struct {
	//migrator:embedded mode="inline" prefix="main_"
	PrefixedTenantAware
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`
}

func TestNestedEmbeddedFieldsWithPrefixes(t *testing.T) {
	c := qt.New(t)

	// Parse the test file to get the schema
	database := ParseFile("embedded_bug_reproduction_test.go")

	// Process embedded fields to get all fields
	allFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	// Find the prefixed_test table fields
	var prefixedTestFields []Field
	for _, field := range allFields {
		if field.StructName == "PrefixedTest" {
			prefixedTestFields = append(prefixedTestFields, field)
		}
	}

	// Expected fields with combined prefixes
	expectedFields := map[string]bool{
		"main_entity_id": true, // main_ + entity_ + id (nested prefix combination)
		"main_tenant_id": true, // main_ + tenant_id
		"name":           true, // Direct field (no prefix)
	}

	actualFields := make(map[string]Field)
	for _, field := range prefixedTestFields {
		actualFields[field.Name] = field
	}

	for expectedField := range expectedFields {
		field, exists := actualFields[expectedField]
		c.Assert(exists, qt.IsTrue, qt.Commentf("Expected field '%s' not found in PrefixedTest table. Actual fields: %v", expectedField, getFieldNames(prefixedTestFields)))

		// The nested id field should still be primary even with prefixes
		if expectedField == "main_entity_id" {
			c.Assert(field.Primary, qt.IsTrue, qt.Commentf("The prefixed id field should still be marked as primary key"))
		}
	}

	// Verify no unexpected fields
	c.Assert(len(prefixedTestFields), qt.Equals, len(expectedFields), qt.Commentf("Expected %d fields, got %d. Actual fields: %v", len(expectedFields), len(prefixedTestFields), getFieldNames(prefixedTestFields)))
}

// Helper function to get field names for debugging
func getFieldNames(fields []Field) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

// Test the exact scenario from GitHub issue #49
func TestGitHubIssue49ExactScenario(t *testing.T) {
	c := qt.New(t)

	// Create a temporary test file with the exact structs from the issue
	testContent := `package test

// EntityID represents a common ID structure with primary key
type EntityID struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string ` + "`json:\"id\" db:\"id\" userinput:\"false\"`" + `
}

// TenantAwareEntityID embeds EntityID and adds tenant/user references
type TenantAwareEntityID struct {
	//migrator:embedded mode="inline"
	EntityID
	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true" foreign="tenants(id)" foreign_key_name="fk_entity_tenant"
	TenantID string ` + "`json:\"tenant_id\" db:\"tenant_id\" userinput:\"false\"`" + `
	//migrator:schema:field name="user_id" type="TEXT" not_null="true" foreign="users(id)" foreign_key_name="fk_entity_user"
	UserID string ` + "`json:\"user_id\" db:\"user_id\" userinput:\"false\"`" + `
}

//migrator:schema:table name="users"
type User struct {
	//migrator:embedded mode="inline"
	TenantAwareEntityID
	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string ` + "`json:\"email\" db:\"email\"`" + `
	//migrator:schema:field name="password_hash" type="TEXT" not_null="true"
	PasswordHash string ` + "`json:\"password_hash\" db:\"password_hash\"`" + `
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `
	//migrator:schema:field name="role" type="TEXT" not_null="true" default="user"
	Role string ` + "`json:\"role\" db:\"role\"`" + `
	//migrator:schema:field name="is_active" type="BOOLEAN" not_null="true" default="true"
	IsActive bool ` + "`json:\"is_active\" db:\"is_active\"`" + `
	//migrator:schema:field name="last_login_at" type="TIMESTAMP"
	LastLoginAt *string ` + "`json:\"last_login_at\" db:\"last_login_at\"`" + `
	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true"
	CreatedAt string ` + "`json:\"created_at\" db:\"created_at\"`" + `
	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true"
	UpdatedAt string ` + "`json:\"updated_at\" db:\"updated_at\"`" + `
}

//migrator:schema:table name="tenants"
type Tenant struct {
	//migrator:embedded mode="inline"
	EntityID
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `
}

//migrator:schema:table name="areas"
type Area struct {
	//migrator:embedded mode="inline"
	TenantAwareEntityID
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `
}
`

	// Write the test content to a temporary file
	tmpDir := t.TempDir()
	testFile := tmpDir + "/issue49_test.go"
	err := os.WriteFile(testFile, []byte(testContent), 0600)
	c.Assert(err, qt.IsNil)

	// Parse the test file
	database := ParseFile(testFile)

	// Process embedded fields
	allFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	// Verify that the users table has the id field (this was the main issue)
	var usersFields []Field
	for _, field := range allFields {
		if field.StructName == "User" {
			usersFields = append(usersFields, field)
		}
	}

	// Check that the id field exists and is primary
	var hasIDField bool
	var idField Field
	for _, field := range usersFields {
		if field.Name == "id" {
			hasIDField = true
			idField = field
			break
		}
	}

	// This was the main bug - the id field was missing
	c.Assert(hasIDField, qt.IsTrue, qt.Commentf("Users table should have an 'id' field from nested embedded EntityID"))
	c.Assert(idField.Primary, qt.IsTrue, qt.Commentf("The id field should be marked as primary key"))
	c.Assert(idField.Type, qt.Equals, "TEXT")

	// Verify all expected fields are present
	expectedFields := []string{"id", "tenant_id", "user_id", "email", "password_hash", "name", "role", "is_active", "last_login_at", "created_at", "updated_at"}
	actualFieldNames := make(map[string]bool)
	for _, field := range usersFields {
		actualFieldNames[field.Name] = true
	}

	for _, expectedField := range expectedFields {
		c.Assert(actualFieldNames[expectedField], qt.IsTrue, qt.Commentf("Expected field '%s' not found in users table", expectedField))
	}

	// Verify that areas table also has the id field (this would also fail before the fix)
	var areasFields []Field
	for _, field := range allFields {
		if field.StructName == "Area" {
			areasFields = append(areasFields, field)
		}
	}

	var areasHasIDField bool
	for _, field := range areasFields {
		if field.Name == "id" && field.Primary {
			areasHasIDField = true
			break
		}
	}
	c.Assert(areasHasIDField, qt.IsTrue, qt.Commentf("Areas table should have an 'id' field from nested embedded EntityID"))

	// Verify that tenants table has the id field (this should work even before the fix)
	var tenantsFields []Field
	for _, field := range allFields {
		if field.StructName == "Tenant" {
			tenantsFields = append(tenantsFields, field)
		}
	}

	var tenantsHasIDField bool
	for _, field := range tenantsFields {
		if field.Name == "id" && field.Primary {
			tenantsHasIDField = true
			break
		}
	}
	c.Assert(tenantsHasIDField, qt.IsTrue, qt.Commentf("Tenants table should have an 'id' field from embedded EntityID"))

	t.Logf("✅ GitHub issue #49 has been fixed!")
	t.Logf("✅ Users table now correctly includes the 'id' primary key field from nested embedded EntityID")
	t.Logf("✅ Areas table now correctly includes the 'id' primary key field from nested embedded EntityID")
	t.Logf("✅ All nested embedded fields are properly processed")
}
