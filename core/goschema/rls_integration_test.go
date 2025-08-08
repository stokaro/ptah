package goschema_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
)

// Helper functions for test file management
func writeTestFile(filename, content string) error {
	return os.WriteFile(filename, []byte(content), 0600)
}

func cleanupTestFile(filename string) {
	os.Remove(filename)
}

func TestRLSAndFunctionIntegration_EndToEnd(t *testing.T) {
	c := qt.New(t)

	// Create a test Go file content with RLS and function annotations
	testGoContent := `package testpkg

//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;" comment="Sets the current tenant context for RLS"
//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"
//migrator:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//migrator:schema:table name="users" comment="User accounts table"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string ` + "`json:\"tenant_id\" db:\"tenant_id\"`" + `

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string ` + "`json:\"email\" db:\"email\"`" + `

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `
}

//migrator:schema:rls:enable table="products" comment="Enable RLS for product isolation"
//migrator:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures products are isolated by tenant"
//migrator:schema:table name="products" comment="Product catalog table"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string ` + "`json:\"tenant_id\" db:\"tenant_id\"`" + `

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `

	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0"
	Price float64 ` + "`json:\"price\" db:\"price\"`" + `

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64 ` + "`json:\"user_id\" db:\"user_id\"`" + `
}
`

	// Write the test file
	testFile := "test_rls_integration.go"
	err := writeTestFile(testFile, testGoContent)
	c.Assert(err, qt.IsNil)
	defer cleanupTestFile(testFile)

	// Parse the file
	database := goschema.ParseFile(testFile)

	// Verify functions were parsed correctly
	c.Assert(database.Functions, qt.HasLen, 2)

	setTenantFunc := findFunction(database.Functions, "set_tenant_context")
	c.Assert(setTenantFunc, qt.IsNotNil)
	c.Assert(setTenantFunc.Parameters, qt.Equals, "tenant_id_param TEXT")
	c.Assert(setTenantFunc.Returns, qt.Equals, "VOID")
	c.Assert(setTenantFunc.Language, qt.Equals, "plpgsql")
	c.Assert(setTenantFunc.Security, qt.Equals, "DEFINER")
	c.Assert(setTenantFunc.Comment, qt.Equals, "Sets the current tenant context for RLS")

	getTenantFunc := findFunction(database.Functions, "get_current_tenant_id")
	c.Assert(getTenantFunc, qt.IsNotNil)
	c.Assert(getTenantFunc.Returns, qt.Equals, "TEXT")
	c.Assert(getTenantFunc.Language, qt.Equals, "plpgsql")
	c.Assert(getTenantFunc.Volatility, qt.Equals, "STABLE")
	c.Assert(getTenantFunc.Comment, qt.Equals, "Gets the current tenant ID from session")

	// Verify RLS enabled tables were parsed correctly
	c.Assert(database.RLSEnabledTables, qt.HasLen, 2)

	usersRLS := findRLSEnabledTable(database.RLSEnabledTables, "users")
	c.Assert(usersRLS, qt.IsNotNil)
	c.Assert(usersRLS.Comment, qt.Equals, "Enable RLS for multi-tenant isolation")

	productsRLS := findRLSEnabledTable(database.RLSEnabledTables, "products")
	c.Assert(productsRLS, qt.IsNotNil)
	c.Assert(productsRLS.Comment, qt.Equals, "Enable RLS for product isolation")

	// Verify RLS policies were parsed correctly
	c.Assert(database.RLSPolicies, qt.HasLen, 2)

	userPolicy := findRLSPolicy(database.RLSPolicies, "user_tenant_isolation")
	c.Assert(userPolicy, qt.IsNotNil)
	c.Assert(userPolicy.Table, qt.Equals, "users")
	c.Assert(userPolicy.PolicyFor, qt.Equals, "ALL")
	c.Assert(userPolicy.ToRoles, qt.Equals, "inventario_app")
	c.Assert(userPolicy.UsingExpression, qt.Equals, "tenant_id = get_current_tenant_id()")
	c.Assert(userPolicy.Comment, qt.Equals, "Ensures users can only access their tenant's data")

	productPolicy := findRLSPolicy(database.RLSPolicies, "product_tenant_isolation")
	c.Assert(productPolicy, qt.IsNotNil)
	c.Assert(productPolicy.Table, qt.Equals, "products")
	c.Assert(productPolicy.PolicyFor, qt.Equals, "ALL")
	c.Assert(productPolicy.ToRoles, qt.Equals, "inventario_app")
	c.Assert(productPolicy.UsingExpression, qt.Equals, "tenant_id = get_current_tenant_id()")
	c.Assert(productPolicy.WithCheckExpression, qt.Equals, "tenant_id = get_current_tenant_id()")
	c.Assert(productPolicy.Comment, qt.Equals, "Ensures products are isolated by tenant")

	// Generate PostgreSQL SQL and verify it contains the expected statements
	statements := renderer.GetOrderedCreateStatements(&database, "postgresql")
	c.Assert(statements, qt.Not(qt.HasLen), 0)

	sqlOutput := strings.Join(statements, "\n")

	// Verify function creation SQL
	c.Assert(sqlOutput, qt.Contains, "CREATE OR REPLACE FUNCTION set_tenant_context(tenant_id_param TEXT)")
	c.Assert(sqlOutput, qt.Contains, "RETURNS VOID")
	c.Assert(sqlOutput, qt.Contains, "LANGUAGE plpgsql SECURITY DEFINER")
	c.Assert(sqlOutput, qt.Contains, "PERFORM set_config('app.current_tenant_id', tenant_id_param, false)")

	c.Assert(sqlOutput, qt.Contains, "CREATE OR REPLACE FUNCTION get_current_tenant_id()")
	c.Assert(sqlOutput, qt.Contains, "LANGUAGE plpgsql STABLE")
	c.Assert(sqlOutput, qt.Contains, "current_setting('app.current_tenant_id', true)")

	// Verify RLS enablement SQL
	c.Assert(sqlOutput, qt.Contains, "ALTER TABLE users ENABLE ROW LEVEL SECURITY")
	c.Assert(sqlOutput, qt.Contains, "ALTER TABLE products ENABLE ROW LEVEL SECURITY")

	// Verify RLS policy creation SQL
	c.Assert(sqlOutput, qt.Contains, "CREATE POLICY user_tenant_isolation ON users")
	c.Assert(sqlOutput, qt.Contains, "FOR ALL TO inventario_app")
	c.Assert(sqlOutput, qt.Contains, "USING (tenant_id = get_current_tenant_id())")

	c.Assert(sqlOutput, qt.Contains, "CREATE POLICY product_tenant_isolation ON products")
	c.Assert(sqlOutput, qt.Contains, "WITH CHECK (tenant_id = get_current_tenant_id())")

	// Verify table creation SQL is still present
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE users")
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE products")
	c.Assert(sqlOutput, qt.Contains, "FOREIGN KEY (user_id) REFERENCES users(id)")
}

func TestRLSAndFunctionIntegration_MySQLSkipsPostgreSQLFeatures(t *testing.T) {
	c := qt.New(t)

	// Test that MySQL correctly skips PostgreSQL-specific features
	testGoContent := `package testpkg

//migrator:schema:function name="test_func" returns="INTEGER" language="sql"
//migrator:schema:rls:enable table="test_table"
//migrator:schema:rls:policy name="test_policy" table="test_table" for="ALL" to="app_user" using="user_id = current_user_id()"
//migrator:schema:table name="test_table"
type TestTable struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}
`

	testFile := "test_mysql_skip.go"
	err := writeTestFile(testFile, testGoContent)
	c.Assert(err, qt.IsNil)
	defer cleanupTestFile(testFile)

	database := goschema.ParseFile(testFile)

	// Verify that the functions and RLS policies were parsed
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)

	// Generate MySQL SQL - PostgreSQL-specific features should be skipped
	statements := renderer.GetOrderedCreateStatements(&database, "mysql")
	sqlOutput := strings.Join(statements, "\n")

	// Verify that PostgreSQL-specific features are not included in MySQL output
	c.Assert(sqlOutput, qt.Not(qt.Contains), "CREATE FUNCTION")
	c.Assert(sqlOutput, qt.Not(qt.Contains), "CREATE POLICY")
	c.Assert(sqlOutput, qt.Not(qt.Contains), "ENABLE ROW LEVEL SECURITY")

	// But table creation should still work
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE test_table")
	c.Assert(sqlOutput, qt.Contains, "id INTEGER PRIMARY KEY")
}

// Helper functions
func findFunction(functions []goschema.Function, name string) *goschema.Function {
	for _, f := range functions {
		if f.Name == name {
			return &f
		}
	}
	return nil
}

func findRLSEnabledTable(tables []goschema.RLSEnabledTable, tableName string) *goschema.RLSEnabledTable {
	for _, t := range tables {
		if t.Table == tableName {
			return &t
		}
	}
	return nil
}

func findRLSPolicy(policies []goschema.RLSPolicy, name string) *goschema.RLSPolicy {
	for _, p := range policies {
		if p.Name == name {
			return &p
		}
	}
	return nil
}
