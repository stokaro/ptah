package goschema_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/goschema/internal/parseutils"
)

func mustParseSource(c *qt.C, filename string, source any) goschema.Database {
	c.Helper()
	db, err := goschema.ParseSource(filename, source)
	c.Assert(err, qt.IsNil)
	return db
}

func mustParseFile(c *qt.C, filename string) goschema.Database {
	c.Helper()
	db, err := goschema.ParseFile(filename)
	c.Assert(err, qt.IsNil)
	return db
}

func TestParseKeyValueComment_SimplifiedSyntax(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		comment  string
		expected map[string]string
	}{
		{
			name:    "Traditional syntax with quotes",
			comment: `//migrator:schema:field name="id" type="SERIAL" primary="true" not_null="true"`,
			expected: map[string]string{
				"name":     "id",
				"type":     "SERIAL",
				"primary":  "true",
				"not_null": "true",
			},
		},
		{
			name:    "Simplified syntax without quotes",
			comment: `//migrator:schema:field name="id" type="SERIAL" primary not_null`,
			expected: map[string]string{
				"name":     "id",
				"type":     "SERIAL",
				"primary":  "true",
				"not_null": "true",
			},
		},
		{
			name:    "Mixed syntax",
			comment: `//migrator:schema:field name="email" type="VARCHAR(255)" unique not_null index default="test@example.com"`,
			expected: map[string]string{
				"name":     "email",
				"type":     "VARCHAR(255)",
				"unique":   "true",
				"not_null": "true",
				"index":    "true",
				"default":  "test@example.com",
			},
		},
		{
			name:    "Boolean attributes only",
			comment: `//migrator:schema:field primary unique not_null auto_increment`,
			expected: map[string]string{
				"primary":        "true",
				"unique":         "true",
				"not_null":       "true",
				"auto_increment": "true",
			},
		},
		{
			name:    "Platform-specific overrides with simplified syntax",
			comment: `//migrator:schema:field name="data" type="JSONB" not_null platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT"`,
			expected: map[string]string{
				"name":                  "data",
				"type":                  "JSONB",
				"not_null":              "true",
				"platform.mysql.type":   "JSON",
				"platform.mariadb.type": "LONGTEXT",
			},
		},
		{
			name:    "Nullable attribute",
			comment: `//migrator:schema:field name="description" type="TEXT" nullable`,
			expected: map[string]string{
				"name":     "description",
				"type":     "TEXT",
				"nullable": "true",
			},
		},
		{
			name:    "Complex check constraint with simplified booleans",
			comment: `//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null check="price > 0" index`,
			expected: map[string]string{
				"name":     "price",
				"type":     "DECIMAL(10,2)",
				"not_null": "true",
				"check":    "price > 0",
				"index":    "true",
			},
		},
		{
			name:    "Embedded field with simplified syntax",
			comment: `//migrator:embedded mode="inline" prefix="audit_"`,
			expected: map[string]string{
				"mode":   "inline",
				"prefix": "audit_",
			},
		},
		{
			name:    "Should not treat non-boolean words as booleans",
			comment: `//migrator:schema:field name="status" type="VARCHAR(50)" default="active"`,
			expected: map[string]string{
				"name":    "status",
				"type":    "VARCHAR(50)",
				"default": "active",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseutils.ParseKeyValueComment(tt.comment)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

func TestParseSource_FieldIdentityAttributes(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", `
package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="int" identity_generation="BY_DEFAULT" identity_start="10" identity_increment="5" identity_options="START WITH 10 INCREMENT BY 5 CACHE 3"
	ID int64
}
`)

	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(db.Fields[0].IdentityGeneration, qt.Equals, "BY_DEFAULT")
	c.Assert(db.Fields[0].IdentityStart, qt.Equals, "10")
	c.Assert(db.Fields[0].IdentityIncrement, qt.Equals, "5")
	c.Assert(db.Fields[0].IdentityOptions, qt.Equals, "START WITH 10 INCREMENT BY 5 CACHE 3")
}

func TestParseSource_FieldIdentityAttributesRejectInvalidGeneration(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.ParseSource("schema.go", `
package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="int" identity_generation="BY_DEFUALT"
	ID int64
}
`)
	c.Assert(err, qt.ErrorMatches, `invalid identity_generation "BY_DEFUALT".*`)
}

func TestParseSource_FieldIdentityOptionsDefaultGeneration(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema.go", `
package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="int" identity_options="MINVALUE 0 START WITH 0"
	ID int64
}
`)

	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(db.Fields[0].IdentityGeneration, qt.Equals, "BY_DEFAULT")
	c.Assert(db.Fields[0].IdentityOptions, qt.Equals, "MINVALUE 0 START WITH 0")
}

func TestParseSchemaObjectAnnotations(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "schema_objects.go", `
package test

//migrator:schema:view name="active_users" body="SELECT id FROM users WHERE deleted_at IS NULL" with_check="true" comment="Active users"
//migrator:schema:matview name="user_stats" body="SELECT id, COUNT(*) FROM users GROUP BY id"
//migrator:schema:trigger name="set_updated_at" table="users" timing="before" event="update" body="NEW.updated_at = NOW(); RETURN NEW;"
//migrator:schema:schema name="auth" comment="Authentication schema"
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`)

	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Name, qt.Equals, "active_users")
	c.Assert(db.Views[0].WithCheck, qt.IsTrue)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "user_stats")
	c.Assert(db.MaterializedViews[0].RefreshStrategy, qt.Equals, "manual")
	c.Assert(db.Triggers, qt.HasLen, 1)
	c.Assert(db.Triggers[0].Name, qt.Equals, "set_updated_at")
	c.Assert(db.Triggers[0].Table, qt.Equals, "users")
	c.Assert(db.Triggers[0].Timing, qt.Equals, "BEFORE")
	c.Assert(db.Triggers[0].Event, qt.Equals, "UPDATE")
	c.Assert(db.Triggers[0].ForEach, qt.Equals, "ROW")
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{
		Name:    "auth",
		Comment: "Authentication schema",
	}})
}

func TestParseSchemaObjectAnnotations_RejectsInvalidAttributes(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		annotation string
		want       string
	}{
		{
			name:       "unknown view attribute",
			annotation: `//migrator:schema:view name="active_users" body="SELECT id FROM users" boddy="typo"`,
			want:       "boddy",
		},
		{
			name:       "missing materialized view body",
			annotation: `//migrator:schema:matview name="user_stats"`,
			want:       `missing required annotation attribute "body"`,
		},
		{
			name:       "missing trigger table",
			annotation: `//migrator:schema:trigger name="set_updated_at" timing="before" event="update" body="RETURN NEW;"`,
			want:       `missing required annotation attribute "table"`,
		},
		{
			name:       "missing schema name",
			annotation: `//migrator:schema:schema comment="missing name"`,
			want:       `missing required annotation attribute "name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := goschema.ParseSource("schema_object_invalid.go", `
package test
`+tt.annotation+`
type User struct{}
`)
			c.Assert(err, qt.ErrorMatches, ".*"+tt.want+".*")
		})
	}
}

func TestTrigger_FunctionNameIsTableScoped(t *testing.T) {
	c := qt.New(t)

	userTrigger := goschema.Trigger{Name: "set_updated_at", Table: "public.users"}
	postTrigger := goschema.Trigger{Name: "set_updated_at", Table: "public.posts"}

	c.Assert(userTrigger.FunctionName(), qt.Equals, "ptah_trigger_public_users_set_updated_at")
	c.Assert(postTrigger.FunctionName(), qt.Equals, "ptah_trigger_public_posts_set_updated_at")
	c.Assert(userTrigger.FunctionName(), qt.Not(qt.Equals), postTrigger.FunctionName())
}

func TestParseKeyValueComment_BooleanPatterns(t *testing.T) {
	c := qt.New(t)

	// Test that only known boolean attributes are treated as booleans
	tests := []struct {
		name     string
		comment  string
		attr     string
		expected string
	}{
		{
			name:     "not_null should be boolean",
			comment:  `//migrator:schema:field not_null`,
			attr:     "not_null",
			expected: "true",
		},
		{
			name:     "nullable should be boolean",
			comment:  `//migrator:schema:field nullable`,
			attr:     "nullable",
			expected: "true",
		},
		{
			name:     "primary should be boolean",
			comment:  `//migrator:schema:field primary`,
			attr:     "primary",
			expected: "true",
		},
		{
			name:     "unique should be boolean",
			comment:  `//migrator:schema:field unique`,
			attr:     "unique",
			expected: "true",
		},
		{
			name:     "auto_increment should be boolean",
			comment:  `//migrator:schema:field auto_increment`,
			attr:     "auto_increment",
			expected: "true",
		},
		{
			name:     "index should be boolean",
			comment:  `//migrator:schema:field index`,
			attr:     "index",
			expected: "true",
		},
		{
			name:     "is_ prefix should be boolean",
			comment:  `//migrator:schema:field is_active`,
			attr:     "is_active",
			expected: "true",
		},
		{
			name:     "has_ prefix should be boolean",
			comment:  `//migrator:schema:field has_permission`,
			attr:     "has_permission",
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseutils.ParseKeyValueComment(tt.comment)
			c.Assert(result[tt.attr], qt.Equals, tt.expected)
		})
	}
}

func TestParseKeyValueComment_IgnoreNonBooleans(t *testing.T) {
	c := qt.New(t)

	// Test that non-boolean words are not treated as booleans
	comment := `//migrator:schema:field name="test" type="VARCHAR" migrator schema field table`
	result := parseutils.ParseKeyValueComment(comment)

	// These should not be treated as boolean attributes
	c.Assert(result["migrator"], qt.Equals, "")
	c.Assert(result["schema"], qt.Equals, "")
	c.Assert(result["field"], qt.Equals, "")
	c.Assert(result["table"], qt.Equals, "")

	// These should be parsed correctly
	c.Assert(result["name"], qt.Equals, "test")
	c.Assert(result["type"], qt.Equals, "VARCHAR")
}

func TestParseKeyValueComment_PrecedenceRules(t *testing.T) {
	c := qt.New(t)

	// Test that explicit key=value takes precedence over standalone boolean
	comment := `//migrator:schema:field not_null not_null="false"`
	result := parseutils.ParseKeyValueComment(comment)

	// The explicit not_null="false" should take precedence over standalone not_null
	c.Assert(result["not_null"], qt.Equals, "false")
}

func TestParseFile_EnumHandling(t *testing.T) {
	c := qt.New(t)

	// Create a test file with both enum and non-enum fields
	content := `package entities

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="active" type="BOOLEAN" not_null="true" default_expr="true"
	Active bool

	//migrator:schema:field name="status" type="ENUM" enum="draft,active,discontinued" not_null="true" default="draft"
	Status string
}
`

	// Write to temporary file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "product.go")
	err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine
	c.Assert(err, qt.IsNil)

	// Parse the file
	database := mustParseFile(c, testFile)

	// Should have 4 fields and 1 enum
	c.Assert(database.Fields, qt.HasLen, 4)
	c.Assert(database.Enums, qt.HasLen, 1)

	// Check that non-enum fields have nil Enum values
	for _, field := range database.Fields {
		switch field.Name {
		case "id", "name", "active":
			// These fields should have nil Enum values (not []string{""})
			c.Assert(field.Enum, qt.IsNil, qt.Commentf("Field %s should have nil Enum, got %v", field.Name, field.Enum))
		case "status":
			// This field should have enum values
			c.Assert(field.Enum, qt.DeepEquals, []string{"draft", "active", "discontinued"})
			c.Assert(field.Type, qt.Equals, "enum_product_status")
		}
	}

	// Check the global enum
	c.Assert(database.Enums[0].Name, qt.Equals, "enum_product_status")
	c.Assert(database.Enums[0].Values, qt.DeepEquals, []string{"draft", "active", "discontinued"})
}

func TestParseFile_TableSchemaAttribute(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="users" schema="auth"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary
	ID int64
}
`

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "user.go")
	err := os.WriteFile(testFile, []byte(content), 0o600)
	c.Assert(err, qt.IsNil)

	database := mustParseFile(c, testFile)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "users")
	c.Assert(database.Tables[0].Schema, qt.Equals, "auth")
	c.Assert(database.Tables[0].QualifiedName(), qt.Equals, "auth.users")
}

func TestParseFile_TableLevelConstraintsUseSchemaQualifiedTables(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="accounts" schema="auth"
type Account struct {
	//migrator:schema:field name="id" type="SERIAL" primary
	ID int64
}

//migrator:schema:table name="users" schema="auth"
//migrator:schema:constraint name="users_status_check" type="CHECK" table="users" check="status <> ''"
//migrator:schema:constraint name="users_account_fk" type="FOREIGN KEY" table="users" columns="account_id" foreign_table="accounts" foreign_column="id"
//migrator:schema:rls:enable table="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary
	ID int64
	//migrator:schema:field name="account_id" type="INTEGER"
	AccountID int64
	//migrator:schema:field name="status" type="TEXT"
	//migrator:schema:index name="idx_users_status" fields="status" table="users"
	Status string
}

//migrator:schema:rls:policy name="users_rls" table="auth.users" for="ALL" using="account_id IS NOT NULL"
`

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "user.go")
	err := os.WriteFile(testFile, []byte(content), 0o600)
	c.Assert(err, qt.IsNil)

	database := mustParseFile(c, testFile)
	c.Assert(database.Constraints, qt.HasLen, 2)
	c.Assert(database.Constraints[0].Table, qt.Equals, "auth.users")
	c.Assert(database.Constraints[1].Table, qt.Equals, "auth.users")
	c.Assert(database.Constraints[1].ForeignTable, qt.Equals, "auth.accounts")
	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].TableName, qt.Equals, "auth.users")
	c.Assert(database.RLSEnabledTables, qt.DeepEquals, []goschema.RLSEnabledTable{
		{StructName: "User", Table: "auth.users"},
	})
	c.Assert(database.RLSPolicies, qt.DeepEquals, []goschema.RLSPolicy{
		{StructName: "User", Name: "users_rls", Table: "auth.users", PolicyFor: "ALL", UsingExpression: "account_id IS NOT NULL"},
	})
}

func TestParsePackageRecursively(t *testing.T) {
	c := qt.New(t)

	// Test parsing the stubs directory
	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Verify we found entities (includes all test files in stubs directory)
	c.Assert(result.Tables, qt.HasLen, 16) // All test tables from various test files
	c.Assert(len(result.Fields) > 0, qt.IsTrue)
	c.Assert(len(result.EmbeddedFields) > 0, qt.IsTrue)

	// Verify dependency ordering
	tableNames := make([]string, len(result.Tables))
	for i, table := range result.Tables {
		tableNames[i] = table.Name
	}

	// users should come before articles (articles depends on users)
	usersIndex := slices.Index(tableNames, "users")
	articlesIndex := slices.Index(tableNames, "articles")
	c.Assert(usersIndex < articlesIndex, qt.IsTrue, qt.Commentf("users should come before articles"))

	// Note: categories has a circular dependency (self-reference), so it may come after products
	// This is expected behavior for circular dependencies
	categoriesIndex := slices.Index(tableNames, "categories")
	productsIndex := slices.Index(tableNames, "products")
	// We just verify both tables exist in the result
	c.Assert(categoriesIndex >= 0, qt.IsTrue, qt.Commentf("categories table should be found"))
	c.Assert(productsIndex >= 0, qt.IsTrue, qt.Commentf("products table should be found"))
}

func TestDependencyResolution(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Check that dependencies are correctly identified
	c.Assert(result.Dependencies["articles"], qt.DeepEquals, []string{"users"})
	c.Assert(result.Dependencies["products"], qt.DeepEquals, []string{"categories"})
	c.Assert(result.Dependencies["categories"], qt.DeepEquals, []string{}) // self-reference moved to SelfReferencingForeignKeys

	// Check that self-referencing foreign keys are tracked separately
	c.Assert(result.SelfReferencingForeignKeys["categories"], qt.HasLen, 1)
	selfRefFK := result.SelfReferencingForeignKeys["categories"][0]
	c.Assert(selfRefFK.Foreign, qt.Equals, "categories(id)")
}

func TestDeduplication(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Verify no duplicate tables
	tableNames := make(map[string]int)
	for _, table := range result.Tables {
		tableNames[table.Name]++
	}
	for name, count := range tableNames {
		c.Assert(count, qt.Equals, 1, qt.Commentf("Table %s should appear only once", name))
	}

	// Verify no duplicate fields within the same struct
	fieldKeys := make(map[string]int)
	for _, field := range result.Fields {
		key := field.StructName + "." + field.Name
		fieldKeys[key]++
	}
	for key, count := range fieldKeys {
		c.Assert(count, qt.Equals, 1, qt.Commentf("Field %s should appear only once", key))
	}
}

func TestParsePackageRecursively_ErrorCases(t *testing.T) {
	tests := []struct {
		name          string
		rootDir       string
		expectError   bool
		resultChecker qt.Checker
	}{
		{
			name:          "non-existent directory",
			rootDir:       "non-existent-directory",
			expectError:   true,
			resultChecker: qt.IsNil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := goschema.ParseDir(tt.rootDir)
			c.Assert(err == nil, qt.Equals, !tt.expectError, qt.Commentf("Unexpected error value: %v", err))
			c.Assert(result, tt.resultChecker, qt.Commentf("Unexpected result value: %v", result))
		})
	}
}

func TestGetDependencyInfo_EmptyResult(t *testing.T) {
	c := qt.New(t)

	// Create an empty result to test edge case
	result := &goschema.Database{
		Tables:       []goschema.Table{},
		Dependencies: make(map[string][]string),
	}

	info := goschema.GetDependencyInfo(result)

	// Should still contain the headers even with no tables
	c.Assert(info, qt.Contains, "Table Dependencies:")
	c.Assert(info, qt.Contains, "Table Creation Order:")

	// Should not contain any table entries
	lines := strings.Split(info, "\n")
	tableCount := 0
	for _, line := range lines {
		if strings.Contains(line, ": (no dependencies)") || strings.Contains(line, ": depends on") {
			tableCount++
		}
	}
	c.Assert(tableCount, qt.Equals, 0)
}

func TestGetDependencyInfo(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	info := goschema.GetDependencyInfo(result)

	// Verify the output contains expected sections
	c.Assert(info, qt.Contains, "Table Dependencies:")
	c.Assert(info, qt.Contains, "==================")
	c.Assert(info, qt.Contains, "Table Creation Order:")

	// Verify specific dependency information
	c.Assert(info, qt.Contains, "articles: depends on [users]")
	c.Assert(info, qt.Contains, "products: depends on [categories]")
	c.Assert(info, qt.Contains, "categories: (no dependencies)") // self-reference moved to SelfReferencingForeignKeys

	// Verify tables with no dependencies are marked correctly
	c.Assert(info, qt.Contains, "users: (no dependencies)")

	// Verify table creation order section contains numbered list
	lines := strings.Split(info, "\n")
	var orderSectionFound bool
	for _, line := range lines {
		if strings.Contains(line, "Table Creation Order:") {
			orderSectionFound = true
			continue
		}
		if orderSectionFound && strings.Contains(line, "1. ") {
			// Found the first item in the order list
			c.Assert(line, qt.Matches, `\d+\. \w+`)
			break
		}
	}
	c.Assert(orderSectionFound, qt.IsTrue, qt.Commentf("Should find Table Creation Order section"))
}

func TestParseFunctionComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.Function
	}{
		{
			name:    "Basic function definition",
			comment: `//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"`,
			expected: goschema.Function{
				StructName: "TestStruct",
				Name:       "set_tenant_context",
				Parameters: "tenant_id_param text", // lowercased to match pg_get_function_arguments
				Returns:    "void",                 // lowercased to match pg_get_function_result
				Language:   "plpgsql",
				Security:   "DEFINER",
				Volatility: "VOLATILE", // default
				Body:       "BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;",
			},
		},
		{
			name:    "Function with volatility",
			comment: `//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;"`,
			expected: goschema.Function{
				StructName: "TestStruct",
				Name:       "get_current_tenant_id",
				Returns:    "text", // lowercased
				Language:   "plpgsql",
				Security:   "INVOKER", // default
				Volatility: "STABLE",
				Body:       "BEGIN RETURN current_setting('app.current_tenant_id', true); END;",
			},
		},
		{
			name:    "Function with comment",
			comment: `//migrator:schema:function name="test_func" returns="INTEGER" language="sql" comment="Test function for unit tests"`,
			expected: goschema.Function{
				StructName: "TestStruct",
				Name:       "test_func",
				Returns:    "integer", // lowercased
				Language:   "sql",
				Security:   "INVOKER",  // default
				Volatility: "VOLATILE", // default
				Comment:    "Test function for unit tests",
			},
		},
		{
			name:    "Empty language defaults to plpgsql",
			comment: `//migrator:schema:function name="no_lang" returns="VOID" body="BEGIN END;"`,
			expected: goschema.Function{
				StructName: "TestStruct",
				Name:       "no_lang",
				Returns:    "void",    // lowercased
				Language:   "plpgsql", // defaulted
				Security:   "INVOKER",
				Volatility: "VOLATILE",
				Body:       "BEGIN END;",
			},
		},
		{
			name:    "Mixed-case attributes are normalized",
			comment: `//migrator:schema:function name="mixed_case" returns="VOID" language="PLPGSQL" security="Definer" volatility="Stable" body="BEGIN END;"`,
			expected: goschema.Function{
				StructName: "TestStruct",
				Name:       "mixed_case",
				Returns:    "void",    // ToLower
				Language:   "plpgsql", // ToLower
				Security:   "DEFINER", // ToUpper
				Volatility: "STABLE",  // ToUpper
				Body:       "BEGIN END;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Drive the real parseFunctionComment through ParseFile so the
			// canonicalization runs end-to-end. Embedding the annotation in
			// a synthetic Go source file is the path real-world annotations
			// take and is what TestParseRLSPolicyComment also does.
			src := "package p\n" + tt.comment + "\ntype TestStruct struct {}\n"
			tmp := t.TempDir() + "/fn.go"
			c.Assert(os.WriteFile(tmp, []byte(src), 0o644), qt.IsNil) //nolint:gosec // 0644 is fine for a test fixture

			db := mustParseFile(c, tmp)
			c.Assert(db.Functions, qt.HasLen, 1)
			c.Assert(db.Functions[0], qt.DeepEquals, tt.expected)
		})
	}
}

func TestParseRLSPolicyComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.RLSPolicy
	}{
		{
			name:    "Basic RLS policy",
			comment: `//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()"`,
			expected: goschema.RLSPolicy{
				StructName:      "TestStruct",
				Name:            "user_tenant_isolation",
				Table:           "users",
				PolicyFor:       "ALL",
				ToRoles:         "inventario_app",
				UsingExpression: "tenant_id = get_current_tenant_id()",
			},
		},
		{
			name:    "RLS policy with WITH CHECK",
			comment: `//migrator:schema:rls:policy name="insert_policy" table="products" for="INSERT" to="app_user" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()"`,
			expected: goschema.RLSPolicy{
				StructName:          "TestStruct",
				Name:                "insert_policy",
				Table:               "products",
				PolicyFor:           "INSERT",
				ToRoles:             "app_user",
				UsingExpression:     "tenant_id = get_current_tenant_id()",
				WithCheckExpression: "tenant_id = get_current_tenant_id()",
			},
		},
		{
			name:    "RLS policy with comment",
			comment: `//migrator:schema:rls:policy name="select_policy" table="orders" for="SELECT" to="PUBLIC" using="user_id = current_user_id()" comment="Allow users to see only their orders"`,
			expected: goschema.RLSPolicy{
				StructName:      "TestStruct",
				Name:            "select_policy",
				Table:           "orders",
				PolicyFor:       "SELECT",
				ToRoles:         "PUBLIC",
				UsingExpression: "user_id = current_user_id()",
				Comment:         "Allow users to see only their orders",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			kv := parseutils.ParseKeyValueComment(tt.comment)
			policy := goschema.RLSPolicy{
				StructName:          "TestStruct",
				Name:                kv["name"],
				Table:               kv["table"],
				PolicyFor:           kv["for"],
				ToRoles:             kv["to"],
				UsingExpression:     kv["using"],
				WithCheckExpression: kv["with_check"],
				Comment:             kv["comment"],
			}

			c.Assert(policy, qt.DeepEquals, tt.expected)
		})
	}
}

func TestParseRLSEnableComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.RLSEnabledTable
	}{
		{
			name:    "Basic RLS enable",
			comment: `//migrator:schema:rls:enable table="users"`,
			expected: goschema.RLSEnabledTable{
				StructName: "TestStruct",
				Table:      "users",
			},
		},
		{
			name:    "RLS enable with comment",
			comment: `//migrator:schema:rls:enable table="products" comment="Enable RLS for multi-tenant isolation"`,
			expected: goschema.RLSEnabledTable{
				StructName: "TestStruct",
				Table:      "products",
				Comment:    "Enable RLS for multi-tenant isolation",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			kv := parseutils.ParseKeyValueComment(tt.comment)
			rlsEnabled := goschema.RLSEnabledTable{
				StructName: "TestStruct",
				Table:      kv["table"],
				Comment:    kv["comment"],
			}

			c.Assert(rlsEnabled, qt.DeepEquals, tt.expected)
		})
	}
}

func TestParseSource_ConstraintComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.Constraint
	}{
		{
			name:    "EXCLUDE constraint with all fields",
			comment: `//migrator:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" using="gist" elements="room_id WITH =, during WITH &&" condition="is_active = true" comment="Prevent overlapping bookings"`,
			expected: goschema.Constraint{
				StructName:      "TestStruct",
				Name:            "no_overlapping_bookings",
				Type:            "EXCLUDE",
				UsingMethod:     "gist",
				ExcludeElements: "room_id WITH =, during WITH &&",
				WhereCondition:  "is_active = true",
				Comment:         "Prevent overlapping bookings",
			},
		},
		{
			name:    "EXCLUDE constraint without WHERE clause",
			comment: `//migrator:schema:constraint name="unique_locations" type="EXCLUDE" using="gist" elements="location WITH &&"`,
			expected: goschema.Constraint{
				StructName:      "TestStruct",
				Name:            "unique_locations",
				Type:            "EXCLUDE",
				UsingMethod:     "gist",
				ExcludeElements: "location WITH &&",
			},
		},
		{
			name:    "CHECK constraint",
			comment: `//migrator:schema:constraint name="positive_price" type="CHECK" check="price > 0"`,
			expected: goschema.Constraint{
				StructName:      "TestStruct",
				Name:            "positive_price",
				Type:            "CHECK",
				CheckExpression: "price > 0",
			},
		},
		{
			name:    "UNIQUE constraint with multiple columns",
			comment: `//migrator:schema:constraint name="unique_user_email" type="UNIQUE" columns="user_id, email"`,
			expected: goschema.Constraint{
				StructName: "TestStruct",
				Name:       "unique_user_email",
				Type:       "UNIQUE",
				Columns:    []string{"user_id", "email"},
			},
		},
		{
			name:    "FOREIGN KEY constraint",
			comment: `//migrator:schema:constraint name="fk_user" type="FOREIGN KEY" columns="user_id" foreign_table="users" foreign_column="id" on_delete="CASCADE"`,
			expected: goschema.Constraint{
				StructName:    "TestStruct",
				Name:          "fk_user",
				Type:          "FOREIGN KEY",
				Columns:       []string{"user_id"},
				ForeignTable:  "users",
				ForeignColumn: "id",
				OnDelete:      "CASCADE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			source := "package test\n\n" + tt.comment + "\ntype TestStruct struct{}\n"
			db := mustParseSource(c, "constraints.go", source)

			c.Assert(db.Constraints, qt.HasLen, 1)
			constraint := db.Constraints[0]

			c.Assert(constraint.StructName, qt.Equals, tt.expected.StructName)
			c.Assert(constraint.Name, qt.Equals, tt.expected.Name)
			c.Assert(constraint.Type, qt.Equals, tt.expected.Type)
			c.Assert(constraint.UsingMethod, qt.Equals, tt.expected.UsingMethod)
			c.Assert(constraint.ExcludeElements, qt.Equals, tt.expected.ExcludeElements)
			c.Assert(constraint.WhereCondition, qt.Equals, tt.expected.WhereCondition)
			c.Assert(constraint.CheckExpression, qt.Equals, tt.expected.CheckExpression)
			c.Assert(constraint.Columns, qt.DeepEquals, tt.expected.Columns)
			c.Assert(constraint.ForeignTable, qt.Equals, tt.expected.ForeignTable)
			c.Assert(constraint.ForeignColumn, qt.Equals, tt.expected.ForeignColumn)
			c.Assert(constraint.OnDelete, qt.Equals, tt.expected.OnDelete)
			c.Assert(constraint.Comment, qt.Equals, tt.expected.Comment)
		})
	}
}

// TestParseField_UnknownAttributePanics verifies that field annotations
// containing an unrecognized attribute key cause the parser to panic with a
// clear message. This is the safety net that surfaces typos like
// `default_fn`-vs-`default_expr` at parse time instead of silently dropping
// them and producing wrong SQL.
func TestParseField_UnknownAttributePanics(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		annotation  string
		mustContain string
	}{
		{
			name:        "removed default_fn key",
			annotation:  `name="created_at" type="TIMESTAMP" default_fn="NOW()"`,
			mustContain: "default_fn",
		},
		{
			name:        "arbitrary typo",
			annotation:  `name="id" type="SERIAL" primary="true" totally_made_up_attr="oops"`,
			mustContain: "totally_made_up_attr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := `package entities

//migrator:schema:table name="widgets"
type Widget struct {
	//migrator:schema:field ` + tt.annotation + `
	ID int64
}
`
			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "widget.go")
			err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine for tests
			c.Assert(err, qt.IsNil)

			_, err = goschema.ParseFile(testFile)
			c.Assert(err, qt.ErrorMatches, ".*unknown annotation attribute.*")
			c.Assert(err.Error(), qt.Contains, tt.mustContain)
		})
	}
}

// TestParseField_KnownAttributesDoNotPanic verifies that the canonical
// attribute set, including platform.* overrides, parses cleanly without
// triggering the unknown-attribute panic.
func TestParseField_KnownAttributesDoNotPanic(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="widgets"
type Widget struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true" auto_increment="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true" default="x" comment="widget name" platform.mysql.type="VARCHAR(191)"
	Name string

	//migrator:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string
}
`
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "widget.go")
	err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine for tests
	c.Assert(err, qt.IsNil)

	database := mustParseFile(c, testFile)
	c.Assert(database.Fields, qt.HasLen, 3)
}

// TestParseField_ForeignKeyActions verifies that on_delete and on_update
// attributes declared on a //migrator:schema:field annotation are captured on
// the resulting Field (regression test for #117 — these keys were previously
// whitelisted but silently dropped).
func TestParseField_ForeignKeyActions(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="commodities"
type Commodity struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string
}

//migrator:schema:table name="commodity_services"
type CommodityService struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string

	//migrator:schema:field name="commodity_id" type="TEXT" not_null="true" foreign="commodities(id)" foreign_key_name="fk_cs_commodity" on_delete="CASCADE" on_update="RESTRICT"
	CommodityID string
}
`

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "service.go")
	err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine for tests
	c.Assert(err, qt.IsNil)

	database := mustParseFile(c, testFile)

	var fkField *goschema.Field
	for i, f := range database.Fields {
		if f.Name == "commodity_id" {
			fkField = &database.Fields[i]
			break
		}
	}
	c.Assert(fkField, qt.IsNotNil)
	c.Assert(fkField.Foreign, qt.Equals, "commodities(id)")
	c.Assert(fkField.ForeignKeyName, qt.Equals, "fk_cs_commodity")
	c.Assert(fkField.OnDelete, qt.Equals, "CASCADE")
	c.Assert(fkField.OnUpdate, qt.Equals, "RESTRICT")
}

// TestParseDir_EmbeddedRelationFKActions exercises the ParseDir/walker path
// (which expands embedded fields via the internal processEmbeddedFields), to
// pin that on_delete / on_update declared on a //migrator:embedded mode="relation"
// annotation reach the planner-visible Field — a third copy of the embedded
// expansion lives in core/goschema/utils.go and used to drop the actions
// before the fix for #117 landed.
func TestParseDir_EmbeddedRelationFKActions(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE" on_update="RESTRICT"
	Author User
}
`

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "entities.go")
	err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine for tests
	c.Assert(err, qt.IsNil)

	database, err := goschema.ParseDir(tmpDir)
	c.Assert(err, qt.IsNil)

	var authorField *goschema.Field
	for i, f := range database.Fields {
		if f.StructName == "Post" && f.Name == "author_id" {
			authorField = &database.Fields[i]
			break
		}
	}
	c.Assert(authorField, qt.IsNotNil,
		qt.Commentf("expected synthesized author_id field on Post; got fields: %+v", database.Fields))
	c.Assert(authorField.Foreign, qt.Equals, "users(id)")
	c.Assert(authorField.OnDelete, qt.Equals, "CASCADE")
	c.Assert(authorField.OnUpdate, qt.Equals, "RESTRICT")
}

// TestParseField_CheckConstraint verifies that the column-level `check=` and
// optional `check_name=` attributes (issue #112) are accepted by the parser
// and propagated onto goschema.Field. The expression is passed through
// verbatim — the parser does not parse or validate the SQL.
func TestParseField_CheckConstraint(t *testing.T) {
	c := qt.New(t)

	content := `package entities

//migrator:schema:table name="files"
type File struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string

	//migrator:schema:field name="category" type="TEXT" not_null="true" default="other" check="category IN ('photos','invoices','documents','other')"
	Category string

	//migrator:schema:field name="type" type="TEXT" not_null="true" check="type IN ('image','document','video','audio','archive','other')" check_name="files_type_valid"
	Type string
}
`

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "file.go")
	err := os.WriteFile(testFile, []byte(content), 0644) //nolint:gosec // 0644 is fine for tests
	c.Assert(err, qt.IsNil)

	database := mustParseFile(c, testFile)

	var category, typ *goschema.Field
	for i, f := range database.Fields {
		switch f.Name {
		case "category":
			category = &database.Fields[i]
		case "type":
			typ = &database.Fields[i]
		}
	}
	c.Assert(category, qt.IsNotNil)
	c.Assert(category.Check, qt.Equals, "category IN ('photos','invoices','documents','other')")
	c.Assert(category.CheckName, qt.Equals, "")

	c.Assert(typ, qt.IsNotNil)
	c.Assert(typ.Check, qt.Equals, "type IN ('image','document','video','audio','archive','other')")
	c.Assert(typ.CheckName, qt.Equals, "files_type_valid")
}
